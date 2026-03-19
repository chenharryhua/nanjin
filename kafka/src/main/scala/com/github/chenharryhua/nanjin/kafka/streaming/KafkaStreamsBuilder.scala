package com.github.chenharryhua.nanjin.kafka.streaming

import cats.effect.kernel.{Async, Deferred}
import cats.effect.std.{CountDownLatch, Dispatcher}
import cats.syntax.flatMap.{catsSyntaxFlatMapOps, toFlatMapOps}
import cats.syntax.functor.toFunctorOps
import cats.syntax.traverse.toTraverseOps
import com.github.chenharryhua.nanjin.common.HasProperties
import com.github.chenharryhua.nanjin.common.utils.toProperties
import com.github.chenharryhua.nanjin.kafka.{KafkaStreamSettings, SchemaRegistrySettings}
import fs2.Stream
import fs2.concurrent.Channel
import io.circe.{Encoder, Json}
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters.MapHasAsJava
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

private object KafkaStreamsAbnormallyStopped extends Exception("Kafka Streams were stopped abnormally")

final case class StateUpdate(newState: State, oldState: State)

object StateUpdate {

  given Encoder[StateUpdate] = (a: StateUpdate) =>
    Json.obj(
      "oldState" -> Json.fromString(a.oldState.name()),
      "newState" -> Json.fromString(a.newState.name())
    )
}

final class KafkaStreamsBuilder[F[_]] private (
  applicationId: String,
  streamSettings: KafkaStreamSettings,
  srClient: SchemaRegistryClient,
  schemaRegistrySettings: SchemaRegistrySettings,
  top: (StreamsBuilder, StreamsSerde) => Unit,
  startUpTimeout: Duration)(using F: Async[F])
    extends HasProperties {

  final private class StateChange(
    dispatcher: Dispatcher[F],
    latch: CountDownLatch[F],
    stop: Deferred[F, Either[Throwable, Unit]],
    bus: Option[Channel[F, StateUpdate]]
  ) extends KafkaStreams.StateListener {

    override def onChange(newState: State, oldState: State): Unit = {
      val decision: F[Unit] = newState match {
        case State.RUNNING     => latch.release
        case State.NOT_RUNNING => stop.complete(Right(())).void
        case State.ERROR       => stop.complete(Left(KafkaStreamsAbnormallyStopped)).void
        case _                 => F.unit
      }
      dispatcher.unsafeRunSync(bus.traverse(_.send(StateUpdate(newState, oldState))) >> decision)
    }
  }

  override lazy val properties: Map[String, String] =
    streamSettings.withProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId).properties

  private def kickoff(bus: Option[Channel[F, StateUpdate]]): Stream[F, KafkaStreams] = {
    val sc: StreamsConfig = new StreamsConfig(properties.asJava)
    for { // fully initialized kafka-streams
      dispatcher <- Stream.resource[F, Dispatcher[F]](Dispatcher.sequential[F])
      stopSignal <- Stream.eval(F.deferred[Either[Throwable, Unit]])
      kafkaStreams <- Stream
        .bracket(F.blocking(new KafkaStreams(topology, sc)))(ks => F.blocking(ks.close()))
        .evalTap { kss =>
          for {
            latch <- CountDownLatch[F](1)
            _ <- F.blocking(kss.setStateListener(new StateChange(dispatcher, latch, stopSignal, bus)))
            _ <- F.blocking(kss.start())
            _ <- F.timeout(latch.await, startUpTimeout)
          } yield ()
        }
        .interruptWhen(stopSignal)
    } yield kafkaStreams
  }

  /** one object KafkaStreams stream. for interactive state store query
    */
  lazy val singleKafkaStreams: Stream[F, KafkaStreams] = kickoff(None)

  lazy val stateUpdatesStream: Stream[F, StateUpdate] = for {
    bus <- Stream.eval(Channel.unbounded[F, StateUpdate])
    _ <- kickoff(Some(bus))
    state <- bus.stream
  } yield state

  def withStartUpTimeout(value: FiniteDuration): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      applicationId = applicationId,
      streamSettings = streamSettings,
      srClient = srClient,
      schemaRegistrySettings = schemaRegistrySettings,
      top = top,
      startUpTimeout = value
    )

  def withProperty(key: String, value: String): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      applicationId = applicationId,
      streamSettings = streamSettings.withProperty(key, value),
      srClient = srClient,
      schemaRegistrySettings = schemaRegistrySettings,
      top = top,
      startUpTimeout = startUpTimeout
    )

  def withProperties(map: Map[String, String]): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      applicationId = applicationId,
      streamSettings = map.foldLeft(streamSettings) { case (ss, (k, v)) => ss.withProperty(k, v) },
      srClient = srClient,
      schemaRegistrySettings = schemaRegistrySettings,
      top = top,
      startUpTimeout = startUpTimeout
    )

  lazy val topology: Topology = {
    val streamsBuilder: StreamsBuilder = new StreamsBuilder()
    val streamsSerde: StreamsSerde = new StreamsSerde(srClient, schemaRegistrySettings)
    top(streamsBuilder, streamsSerde)

    streamsBuilder.build(toProperties(properties))
  }
}

object KafkaStreamsBuilder {
  def apply[F[_]: Async](
    applicationId: String,
    streamSettings: KafkaStreamSettings,
    srClient: SchemaRegistryClient,
    schemaRegistrySettings: SchemaRegistrySettings,
    top: (StreamsBuilder, StreamsSerde) => Unit): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      applicationId = applicationId,
      streamSettings = streamSettings,
      srClient = srClient,
      schemaRegistrySettings = schemaRegistrySettings,
      top = top,
      startUpTimeout = Duration.Inf
    )
}
