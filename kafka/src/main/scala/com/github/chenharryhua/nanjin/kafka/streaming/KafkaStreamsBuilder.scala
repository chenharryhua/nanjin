package com.github.chenharryhua.nanjin.kafka.streaming

import cats.Show
import cats.effect.kernel.{Async, Deferred}
import cats.effect.std.Dispatcher
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.HasProperties
import com.github.chenharryhua.nanjin.common.logging.Log
import com.github.chenharryhua.nanjin.common.utils.toProperties
import com.github.chenharryhua.nanjin.kafka.config.{KafkaStreamSettings, SerdeSettings, StreamsConfigKeys}
import fs2.Stream
import io.circe.{Encoder, Json}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.control.NoStackTrace

final case class KafkaStreamsAbnormallyStopped(applicationId: String)
    extends RuntimeException(s"KafkaStreams($applicationId) were stopped abnormally") with NoStackTrace

final case class KafkaStreamsStartupTimeout(applicationId: String, startupTimeout: Duration)
    extends RuntimeException(s"KafkaStreams($applicationId) did not reach RUNNING within $startupTimeout")
    with NoStackTrace

final case class StateTransition(applicationId: String, oldState: State, newState: State) {
  override def toString: String =
    s"StateTransition(application.id=$applicationId, ${oldState.name()} ==> ${newState.name()})"
}

object StateTransition {
  given Show[StateTransition] = Show.fromToString

  given Encoder[StateTransition] = (a: StateTransition) =>
    Json.obj(
      "event" -> Json.fromString("KafkaStreams.State.Transition"),
      "applicationId" -> Json.fromString(a.applicationId),
      "oldState" -> Json.fromString(a.oldState.name()),
      "newState" -> Json.fromString(a.newState.name())
    )
}

/** Builds and manages a Kafka Streams application with startup monitoring and transition notifications. */
final class KafkaStreamsBuilder[F[_]] private (
  applicationId: String,
  streamSettings: KafkaStreamSettings,
  srClient: SchemaRegistryClient,
  serdeSettings: SerdeSettings,
  top: (StreamsBuilder, StreamsSerde) => Unit,
  startupTimeout: Duration,
  closeTimeout: FiniteDuration,
  log: Log[F])(using F: Async[F])
    extends HasProperties {

  final private class StateTransitionListener(
    dispatcher: Dispatcher[F],
    startup: Deferred[F, Unit],
    stop: Deferred[F, Either[Throwable, Unit]]
  ) extends KafkaStreams.StateListener {

    private def isDispatcherShutdownRace(e: IllegalStateException): Boolean =
      e.getStackTrace.exists(_.getClassName.startsWith("cats.effect.std.Dispatcher"))

    private def runOrIgnoreOnShutdown(fa: F[Unit]): Unit =
      try dispatcher.unsafeRunSync(fa)
      catch {
        case e: IllegalStateException if isDispatcherShutdownRace(e) => ()
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

  /** Stream the managed KafkaStreams instance for interactive state-store inspection. */
  lazy val kafkaStreams: Stream[F, KafkaStreams] = {
    val sc: StreamsConfig = new StreamsConfig(properties.asJava)
    for { // Create and manage the Kafka Streams instance, including listener registration and startup.
      dispatcher <- Stream.resource[F, Dispatcher[F]](Dispatcher.sequential[F])
      startup <- Stream.eval(F.deferred[Unit])
      stop <- Stream.eval(F.deferred[Either[Throwable, Unit]])
      listener = new StateTransitionListener(dispatcher, startup, stop)
      kafkaStreams <- Stream
        .bracket(F.blocking(new KafkaStreams(topology, sc))) { ks =>
          F.blocking(ks.close(closeTimeout.toJava)).void
        }
        .evalTap { kss =>
          for {
            _ <- F.blocking(kss.setStateListener(listener))
            _ <- F.blocking(kss.start())
            _ <- F.timeoutTo(
              startup.get,
              startupTimeout,
              F.raiseError(KafkaStreamsStartupTimeout(applicationId, startupTimeout)))
          } yield ()
        }
        .interruptWhen(stop)
    } yield kafkaStreams
  }

  lazy val runForever: Stream[F, Nothing] = kafkaStreams >> Stream.never[F]

  private def copy(
    streamSettings: KafkaStreamSettings = this.streamSettings,
    startupTimeout: Duration = this.startupTimeout,
    closeTimeout: FiniteDuration = this.closeTimeout,
    log: Log[F] = this.log
  ): KafkaStreamsBuilder[F] = new KafkaStreamsBuilder[F](
    applicationId = this.applicationId,
    streamSettings = streamSettings,
    srClient = this.srClient,
    serdeSettings = this.serdeSettings,
    top = this.top,
    startupTimeout = startupTimeout,
    closeTimeout = closeTimeout,
    log = log
  )

  def withStartUpTimeout(value: FiniteDuration): KafkaStreamsBuilder[F] =
    copy(startupTimeout = value)

  def withCloseTimeout(value: FiniteDuration): KafkaStreamsBuilder[F] =
    copy(closeTimeout = value)

  /** Registers a callback invoked after a Kafka Streams transition is published. */
  def onStateTransition(log: Log[F]): KafkaStreamsBuilder[F] =
    copy(log = log)

  def withProperty(f: StreamsConfigKeys => String, value: String): KafkaStreamsBuilder[F] =
    copy(streamSettings = streamSettings.withProperty(f, value))

  def withProperties(map: Map[String, String]): KafkaStreamsBuilder[F] =
    copy(streamSettings = map.foldLeft(streamSettings) { case (ss, (k, v)) => ss.withProperty(k, v) })

  lazy val topology: Topology = {
    val streamsBuilder: StreamsBuilder = new StreamsBuilder()
    val streamsSerde: StreamsSerde = new StreamsSerde(srClient, serdeSettings)
    top(streamsBuilder, streamsSerde)

    streamsBuilder.build(toProperties(properties))
  }
}

object KafkaStreamsBuilder {
  def apply[F[_]: Async](
    applicationId: String,
    streamSettings: KafkaStreamSettings,
    srClient: SchemaRegistryClient,
    serdeSettings: SerdeSettings,
    top: (StreamsBuilder, StreamsSerde) => Unit): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      applicationId = applicationId,
      streamSettings = streamSettings,
      srClient = srClient,
      serdeSettings = serdeSettings,
      top = top,
      startupTimeout = Duration.Inf,
      closeTimeout = FiniteDuration(30, scala.concurrent.duration.SECONDS),
      log = Log.noop[F]
    )
}
