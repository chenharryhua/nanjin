package com.github.chenharryhua.nanjin.kafka.streaming

import cats.effect.kernel.{Async, Deferred}
import cats.effect.std.{CountDownLatch, Dispatcher}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.kafka.{KafkaStreamSettings, SchemaRegistrySettings}
import fs2.Stream
import fs2.concurrent.Channel
import io.circe.{Encoder, Json}
import io.scalaland.enumz.Enum
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters.MapHasAsJava

private object KafkaStreamsAbnormallyStopped extends Exception("Kafka Streams were stopped abnormally")

final case class StateUpdate(newState: State, oldState: State)

object StateUpdate {
  private val es: Enum[State] = Enum[State]

  implicit final val stateUpdateEncoder: Encoder[StateUpdate] = (a: StateUpdate) =>
    Json.obj(
      ("oldState", Json.fromString(es.getName(a.oldState))),
      ("newState", Json.fromString(es.getName(a.newState)))
    )
}

final class KafkaStreamsBuilder[F[_]] private (
  applicationId: String,
  settings: KafkaStreamSettings,
  schemaRegistrySettings: SchemaRegistrySettings,
  top: (StreamsBuilder, StreamsSerde) => Unit,
  startUpTimeout: Duration)(implicit F: Async[F]) {

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

  private def kickoff(bus: Option[Channel[F, StateUpdate]]): Stream[F, KafkaStreams] = {
    val sc: StreamsConfig = new StreamsConfig(
      settings.withProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId).properties.asJava)
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
  val kafkaStreams: Stream[F, KafkaStreams] = kickoff(None)

  val stateUpdates: Stream[F, StateUpdate] = for {
    bus <- Stream.eval(Channel.unbounded[F, StateUpdate])
    _ <- kickoff(Some(bus))
    state <- bus.stream
  } yield state

  def withStartUpTimeout(value: FiniteDuration): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      applicationId = applicationId,
      settings = settings,
      schemaRegistrySettings = schemaRegistrySettings,
      top = top,
      startUpTimeout = value
    )

  def withProperty(key: String, value: String): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      applicationId = applicationId,
      settings = settings.withProperty(key, value),
      schemaRegistrySettings = schemaRegistrySettings,
      top = top,
      startUpTimeout = startUpTimeout
    )

  lazy val topology: Topology = {
    val streamsBuilder: StreamsBuilder = new StreamsBuilder()
    val streamsSerde: StreamsSerde = new StreamsSerde(schemaRegistrySettings)
    top(streamsBuilder, streamsSerde)
    streamsBuilder.build()
  }
}

object KafkaStreamsBuilder {
  def apply[F[_]: Async](
    applicationId: String,
    settings: KafkaStreamSettings,
    schemaRegistrySettings: SchemaRegistrySettings,
    top: (StreamsBuilder, StreamsSerde) => Unit): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      applicationId = applicationId,
      settings = settings,
      schemaRegistrySettings = schemaRegistrySettings,
      top = top,
      startUpTimeout = Duration.Inf
    )
}
