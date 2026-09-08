package com.github.chenharryhua.nanjin.kafka.streaming

import cats.effect.kernel.{Async, Deferred}
import cats.effect.std.Dispatcher
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.HasProperties
import com.github.chenharryhua.nanjin.common.logging.Log
import com.github.chenharryhua.nanjin.kafka.config.{KafkaStreamSettings, SerdeSettings, StreamsConfigKeys}
import fs2.Stream
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.DurationConverters.ScalaDurationOps

/** Builds and manages a Kafka Streams application with startup monitoring and transition notifications.
  *
  * The topology is described by `buildTopology`, which is handed a `StreamsBuilder` and a `StreamsSerde`
  * (schema-registry-aware serdes). Running the app as an fs2 `Stream` wires a state listener that logs every
  * state change and translates the lifecycle into stream semantics: the stream emits the `KafkaStreams`
  * instance once it reaches `RUNNING` (subject to `startupTimeout`), completes normally on `NOT_RUNNING`, and
  * fails on `ERROR`. The instance is closed (bounded by `closeTimeout`) on release. Configuration is adjusted
  * immutably through the `with*` methods; obtain a builder via `KafkaStreamsBuilder.apply`.
  */
sealed trait KafkaStreamsBuilder[F[_]] extends HasProperties {

  /** The effective streams config, including the application id. */
  override def properties: Map[String, String]

  /** Stream the managed KafkaStreams instance for interactive state-store inspection. */
  def kafkaStreams: Stream[F, KafkaStreams]

  /** Run the streams application until it stops or the stream is interrupted; never emits. Use when you only
    * want the app running as a background stage and do not need the KafkaStreams handle.
    */
  def runForever: Stream[F, Nothing]

  /** Set how long to wait for the app to reach RUNNING before failing with KafkaStreamsStartupTimeout. The
    * default Duration.Inf disables the timeout.
    */
  def withStartupTimeout(value: FiniteDuration): KafkaStreamsBuilder[F]

  /** Set the bound on how long KafkaStreams.close may take on shutdown. */
  def withCloseTimeout(value: FiniteDuration): KafkaStreamsBuilder[F]

  /** Replace the logger used for state transition notifications. */
  def withTransitionLog(log: Log[F]): KafkaStreamsBuilder[F]

  /** Set a single streams config property (key selected from StreamsConfigKeys). */
  def withProperty(f: StreamsConfigKeys => String, value: String): KafkaStreamsBuilder[F]

  /** Merge in a map of streams config properties. */
  def withProperties(map: Map[String, String]): KafkaStreamsBuilder[F]

  /** The built Kafka Streams Topology, produced by running buildTopology against a fresh StreamsBuilder and a
    * schema-registry-aware StreamsSerde. Useful for inspecting or describing the topology without running it.
    */
  def topology: Topology
}

object KafkaStreamsBuilder {

  /** Create a builder with default lifecycle settings: no startup timeout (Duration.Inf), a 30-second close
    * timeout, and a no-op transition logger. Adjust with the with* methods.
    */
  def apply[F[_]: Async](
    applicationId: String,
    streamSettings: KafkaStreamSettings,
    srClient: SchemaRegistryClient,
    serdeSettings: SerdeSettings,
    buildTopology: (StreamsBuilder, StreamsSerde) => Unit): KafkaStreamsBuilder[F] =
    new Impl[F](
      applicationId = applicationId,
      streamSettings = streamSettings,
      srClient = srClient,
      serdeSettings = serdeSettings,
      buildTopology = buildTopology,
      startupTimeout = Duration.Inf,
      closeTimeout = FiniteDuration(30, scala.concurrent.duration.SECONDS),
      log = Log.noop[F]
    )

  final private class Impl[F[_]] private[KafkaStreamsBuilder] (
    applicationId: String,
    streamSettings: KafkaStreamSettings,
    srClient: SchemaRegistryClient,
    serdeSettings: SerdeSettings,
    buildTopology: (StreamsBuilder, StreamsSerde) => Unit,
    startupTimeout: Duration,
    closeTimeout: FiniteDuration,
    log: Log[F])(using F: Async[F])
      extends KafkaStreamsBuilder[F] {

    final private class StateTransitionListener(
      dispatcher: Dispatcher[F],
      startup: Deferred[F, Unit],
      stop: Deferred[F, Either[Throwable, Unit]]
    ) extends KafkaStreams.StateListener {

      private def runOrIgnoreOnShutdown(fa: F[Unit]): Unit =
        try dispatcher.unsafeRunSync(fa)
        catch {
          case _: IllegalStateException => () // dispatcher already shut down
        }

      override def onChange(newState: State, oldState: State): Unit = {
        val st = StateTransition(applicationId = applicationId, oldState = oldState, newState = newState)
        newState match {
          case State.RUNNING =>
            runOrIgnoreOnShutdown(log.good(st) >> startup.complete(()).void)

          case State.PENDING_ERROR => runOrIgnoreOnShutdown(log.warn(st))
          case State.ERROR         =>
            runOrIgnoreOnShutdown(
              log.error(st) >>
                startup.complete(()).void >>
                stop.complete(Left(KafkaStreamsAbnormallyStopped(applicationId))).void)

          case State.PENDING_SHUTDOWN => runOrIgnoreOnShutdown(log.info(st))
          case State.NOT_RUNNING      =>
            runOrIgnoreOnShutdown(
              log.info(st) >>
                startup.complete(()).void >>
                stop.complete(Right(())).void)

          case _ => runOrIgnoreOnShutdown(log.info(st))
        }
      }
    }

    override lazy val properties: Map[String, String] =
      streamSettings.withProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId).properties

    override def kafkaStreams: Stream[F, KafkaStreams] = {
      val sc: StreamsConfig = new StreamsConfig(properties.asJava)
      for {
        dispatcher <- Stream.resource[F, Dispatcher[F]](Dispatcher.sequential[F])
        startup <- Stream.eval(F.deferred[Unit])
        stop <- Stream.eval(F.deferred[Either[Throwable, Unit]])
        listener = new StateTransitionListener(dispatcher, startup, stop)
        kafkaStreams <- Stream
          .bracket(F.blocking(new KafkaStreams(topology, sc))) { ks =>
            if (ks.state().hasCompletedShutdown)
              F.unit
            else
              F.blocking(ks.close(closeTimeout.toJava)).void
          }
          .evalTap { kss =>
            for {
              _ <- F.blocking(kss.setStateListener(listener))
              _ <- F.blocking(kss.start())
              _ <- F.timeoutTo(
                startup.get,
                startupTimeout,
                F.raiseError(KafkaStreamsStartupTimeout(applicationId, startupTimeout))
              )
            } yield ()
          }
          .interruptWhen(stop)
      } yield kafkaStreams
    }

    override def runForever: Stream[F, Nothing] = kafkaStreams >> Stream.never[F]

    private def copy(
      streamSettings: KafkaStreamSettings = this.streamSettings,
      startupTimeout: Duration = this.startupTimeout,
      closeTimeout: FiniteDuration = this.closeTimeout,
      log: Log[F] = this.log
    ): KafkaStreamsBuilder[F] = new Impl[F](
      applicationId = this.applicationId,
      streamSettings = streamSettings,
      srClient = this.srClient,
      serdeSettings = this.serdeSettings,
      buildTopology = this.buildTopology,
      startupTimeout = startupTimeout,
      closeTimeout = closeTimeout,
      log = log
    )

    override def withStartupTimeout(value: FiniteDuration): KafkaStreamsBuilder[F] =
      copy(startupTimeout = value)

    override def withCloseTimeout(value: FiniteDuration): KafkaStreamsBuilder[F] =
      copy(closeTimeout = value)

    override def withTransitionLog(log: Log[F]): KafkaStreamsBuilder[F] =
      copy(log = log)

    override def withProperty(f: StreamsConfigKeys => String, value: String): KafkaStreamsBuilder[F] =
      copy(streamSettings = streamSettings.withProperty(f, value))

    override def withProperties(map: Map[String, String]): KafkaStreamsBuilder[F] =
      copy(streamSettings = map.foldLeft(streamSettings) { case (ss, (k, v)) => ss.withProperty(k, v) })

    override lazy val topology: Topology = {
      val streamsBuilder: StreamsBuilder = new StreamsBuilder()
      val streamsSerde: StreamsSerde = new StreamsSerde(srClient, serdeSettings)
      buildTopology(streamsBuilder, streamsSerde)

      streamsBuilder.build(toProperties(properties))
    }
  }
}
