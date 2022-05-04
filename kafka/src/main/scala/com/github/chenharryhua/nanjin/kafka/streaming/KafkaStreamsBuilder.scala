package com.github.chenharryhua.nanjin.kafka.streaming

import cats.data.Reader
import cats.effect.kernel.{Async, Deferred, Resource}
import cats.effect.std.{CountDownLatch, Dispatcher}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.kafka.KafkaStreamSettings
import fs2.Stream
import fs2.concurrent.Channel
import monocle.function.At.at
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.{KafkaStreams, Topology}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

case object KafkaStreamsAbnormallyStopped extends Exception("Kafka Streams were stopped abnormally")

final class KafkaStreamsBuilder[F[_]] private (
  settings: KafkaStreamSettings,
  top: Reader[StreamsBuilder, Unit],
  localStateStores: List[Reader[StreamsBuilder, StreamsBuilder]],
  startUpTimeout: FiniteDuration)(implicit F: Async[F]) {

  def showSettings: String = settings.show

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

  private def kickoff(
    stop: Deferred[F, Either[Throwable, Unit]],
    bus: Option[Channel[F, State]]): Resource[F, KafkaStreams] = {
    val rrks: Resource[F, Resource[F, KafkaStreams]] = for {
      dispatcher <- Dispatcher[F]
      uks <- Resource.make(F.blocking(new KafkaStreams(topology, settings.javaProperties)))(ks =>
        F.blocking(ks.close()) >> F.blocking(ks.cleanUp()))
    } yield {
      val start: F[KafkaStreams] = for {
        latch <- CountDownLatch[F](1)
        _ <- F.blocking(uks.cleanUp())
        _ <- F.blocking(uks.setStateListener(new StateChange(dispatcher, latch, stop, bus)))
        _ <- F.blocking(uks.start())
        _ <- latch.await
      } yield uks
      Resource.eval(F.timeout(start, startUpTimeout))
    }
    rrks.flatten
  }

  /** one object KafkaStreams stream. for interactive state store query
    */
  val kafkaStreams: Stream[F, KafkaStreams] = for {
    stop <- Stream.eval(F.deferred[Either[Throwable, Unit]])
    ks <- Stream.resource(kickoff(stop, None)).interruptWhen(stop)
  } yield ks

  val stream: Stream[F, State] = for {
    stop <- Stream.eval(F.deferred[Either[Throwable, Unit]])
    bus <- Stream.eval(Channel.unbounded[F, State])
    _ <- Stream.resource(kickoff(stop, Some(bus)))
    state <- bus.stream.interruptWhen(stop)
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
      startUpTimeout = startUpTimeout)

  def addStateStore[S <: StateStore](storeBuilder: StoreBuilder[S]): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      settings = settings,
      top = top,
      localStateStores =
        Reader((sb: StreamsBuilder) => new StreamsBuilder(sb.addStateStore(storeBuilder))) :: localStateStores,
      startUpTimeout = startUpTimeout)

  lazy val topology: Topology = {
    val builder: StreamsBuilder = new StreamsBuilder()
    val lss: StreamsBuilder     = localStateStores.foldLeft(builder)((bd, rd) => rd.run(bd))
    top.run(lss)
    builder.build()
  }
}

object KafkaStreamsBuilder {
  def apply[F[_]: Async](settings: KafkaStreamSettings, top: Reader[StreamsBuilder, Unit]): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](settings, top, Nil, 180.minutes)
}
