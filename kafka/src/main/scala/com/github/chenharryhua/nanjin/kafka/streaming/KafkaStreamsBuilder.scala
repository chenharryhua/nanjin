package com.github.chenharryhua.nanjin.kafka.streaming

import cats.data.{Cont, Reader}
import cats.effect.kernel.{Async, Deferred}
import cats.effect.std.{CountDownLatch, Dispatcher}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.kafka.KafkaStreamSettings
import fs2.Stream
import fs2.concurrent.Channel
import monocle.function.At.at
import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.StoreBuilder

import scala.concurrent.duration.{DurationInt, FiniteDuration}

case object KafkaStreamsAbnormallyStopped extends Exception("Kafka Streams were stopped abnormally")

final class KafkaStreamsBuilder[F[_]] private (
  settings: KafkaStreamSettings,
  top: Reader[StreamsBuilder, Unit],
  localStateStores: Cont[StreamsBuilder, StreamsBuilder],
  startUpTimeout: FiniteDuration)(implicit F: Async[F]) {

  final private class StateChange(
    dispatcher: Dispatcher[F],
    latch: CountDownLatch[F],
    stop: Deferred[F, Either[Throwable, Unit]],
    bus: Option[Channel[F, State]]
  ) extends KafkaStreams.StateListener {

    override def onChange(newState: State, oldState: State): Unit =
      dispatcher.unsafeRunSync(
        bus.traverse(_.send(newState)) >>
          (newState match {
            case State.RUNNING     => latch.release
            case State.NOT_RUNNING => stop.complete(Right(())).void
            case State.ERROR       => stop.complete(Left(KafkaStreamsAbnormallyStopped)).void
            case _                 => F.unit
          })
      )
  }

  private def kickoff(bus: Option[Channel[F, State]]): Stream[F, KafkaStreams] = {
    def startKS(
      ks: KafkaStreams,
      dispatcher: Dispatcher[F],
      stop: Deferred[F, Either[Throwable, Unit]]): F[KafkaStreams] =
      for { // fully initialized KafkaStreams
        latch <- CountDownLatch[F](1)
        _ <- F.blocking(ks.cleanUp())
        _ <- F.delay(ks.setStateListener(new StateChange(dispatcher, latch, stop, bus)))
        _ <- F.blocking(ks.start())
        _ <- F.timeout(latch.await, startUpTimeout)
      } yield ks

    for {
      dispatcher <- Stream.resource[F, Dispatcher[F]](Dispatcher[F])
      stop <- Stream.eval(F.deferred[Either[Throwable, Unit]])
      uks <- Stream.bracket(F.pure(new KafkaStreams(topology, settings.javaProperties)))(ks =>
        F.blocking(ks.close()) >> F.blocking(ks.cleanUp()))
      ks <- Stream.eval(startKS(uks, dispatcher, stop)).interruptWhen(stop)
    } yield ks
  }

  /** one object KafkaStreams stream. for interactive state store query
    */
  val kafkaStreams: Stream[F, KafkaStreams] = kickoff(None)

  val stream: Stream[F, State] = for {
    bus <- Stream.eval(Channel.unbounded[F, State])
    _ <- kickoff(Some(bus))
    state <- bus.stream
  } yield state

  def withStartUpTimeout(value: FiniteDuration): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      settings = settings,
      top = top,
      localStateStores = localStateStores,
      startUpTimeout = value)

  def withProperty(key: String, value: String): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      settings = KafkaStreamSettings.config.composeLens(at(key)).set(Some(value))(settings),
      top = top,
      localStateStores = localStateStores,
      startUpTimeout = startUpTimeout
    )

  def addStateStore[S <: StateStore](storeBuilder: StoreBuilder[S]): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      settings = settings,
      top = top,
      localStateStores = localStateStores.map(sb => new StreamsBuilder(sb.addStateStore(storeBuilder))),
      startUpTimeout = startUpTimeout)

  lazy val topology: Topology = {
    val builder: StreamsBuilder = localStateStores.eval.value
    top.run(builder)
    builder.build()
  }
}

object KafkaStreamsBuilder {
  def apply[F[_]: Async](
    settings: KafkaStreamSettings,
    top: Reader[StreamsBuilder, Unit]): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](settings, top, Cont.defer(new StreamsBuilder()), 180.minutes)
}
