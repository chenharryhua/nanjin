package com.github.chenharryhua.nanjin.kafka.streaming

import cats.Show
import cats.effect.kernel.{Async, Deferred}
import cats.effect.std.Dispatcher
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.HasProperties
import com.github.chenharryhua.nanjin.common.logging.Log
import com.github.chenharryhua.nanjin.common.utils.toProperties
import com.github.chenharryhua.nanjin.kafka.{KafkaStreamSettings, SerdeSettings}
import fs2.Stream
import io.circe.{Encoder, Json}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters.MapHasAsJava

case object KafkaStreamsAbnormallyStopped extends RuntimeException("Kafka Streams were stopped abnormally")

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
  startUpTimeout: Duration,
  log: Log[F])(using F: Async[F])
    extends HasProperties {

  final private class StateTransitionListener(
    dispatcher: Dispatcher[F],
    startup: Deferred[F, Unit],
    stop: Deferred[F, Either[Throwable, Unit]]
  ) extends KafkaStreams.StateListener {

    override def onChange(newState: State, oldState: State): Unit = {
      val st = StateTransition(applicationId = applicationId, oldState = oldState, newState = newState)
      newState match {
        case State.RUNNING =>
          dispatcher.unsafeRunSync(startup.complete(()) >> log.good(st))
        case State.ERROR =>
          dispatcher.unsafeRunSync(
            startup.complete(()) >>
              stop.complete(Left(KafkaStreamsAbnormallyStopped)) >>
              log.error(st))
        // Defensive: Kafka Streams may transition to NOT_RUNNING without this
        // resource initiating shutdown (for example after an internal failure).
        // Ensure the managed Stream terminates in that case.
        case State.NOT_RUNNING =>
          dispatcher.unsafeRunSync(stop.complete(Right(())) >> log.warn(st))
        case _ => ()
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
      stop <- Stream.eval(F.deferred[Either[Throwable, Unit]])
      kafkaStreams <- Stream
        .bracket(F.blocking(new KafkaStreams(topology, sc)))(ks => F.blocking(ks.close()))
        .evalTap { kss =>
          for {
            startup <- F.deferred[Unit]
            _ <- F.blocking(kss.setStateListener(new StateTransitionListener(dispatcher, startup, stop)))
            _ <- F.blocking(kss.start())
            _ <- F.timeout(startup.get, startUpTimeout)
          } yield ()
        }
        .interruptWhen(stop)
    } yield kafkaStreams
  }

  lazy val runForever: Stream[F, Nothing] = kafkaStreams >> Stream.never[F]

  private def copy(
    streamSettings: KafkaStreamSettings = this.streamSettings,
    startUpTimeout: Duration = this.startUpTimeout,
    log: Log[F] = this.log
  ): KafkaStreamsBuilder[F] = new KafkaStreamsBuilder[F](
    applicationId = this.applicationId,
    streamSettings = streamSettings,
    srClient = this.srClient,
    serdeSettings = this.serdeSettings,
    top = this.top,
    startUpTimeout = startUpTimeout,
    log = log
  )

  def withStartUpTimeout(value: FiniteDuration): KafkaStreamsBuilder[F] =
    copy(startUpTimeout = value)

  /** Registers a callback invoked after a Kafka Streams transition is published. */
  def onStateTransition(log: Log[F]): KafkaStreamsBuilder[F] =
    copy(log = log)

  def withProperty(key: String, value: String): KafkaStreamsBuilder[F] =
    copy(streamSettings = streamSettings.withProperty(key, value))

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
      startUpTimeout = Duration.Inf,
      log = Log.noop[F]
    )
}
