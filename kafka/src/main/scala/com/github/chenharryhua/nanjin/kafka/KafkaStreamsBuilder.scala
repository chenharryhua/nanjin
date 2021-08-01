package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.kernel.{Async, Deferred, Resource}
import cats.effect.std.{CountDownLatch, Dispatcher}
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Channel
import monocle.function.At.at
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.{KafkaStreams, Topology}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

final case class KafkaStreamsStoppedException() extends Exception("Kafka Streams were stopped")

final class KafkaStreamsBuilder[F[_]] private (
  settings: KafkaStreamSettings,
  top: Reader[StreamsBuilder, Unit],
  localStateStores: List[Reader[StreamsBuilder, StreamsBuilder]],
  startUpTimeout: FiniteDuration)(implicit F: Async[F]) {
  final private class StreamErrorHandler(dispatcher: Dispatcher[F], err: Deferred[F, Either[Throwable, Unit]])
      extends StreamsUncaughtExceptionHandler {

    override def handle(throwable: Throwable): StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse = {
      dispatcher.unsafeRunSync(err.complete(Left(throwable)))
      StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION
    }
  }

  final private class StateChange(
    dispatcher: Dispatcher[F],
    bus: Channel[F, State],
    latch: CountDownLatch[F],
    stop: Deferred[F, Either[Throwable, Unit]])
      extends KafkaStreams.StateListener {

    override def onChange(newState: State, oldState: State): Unit =
      dispatcher.unsafeRunSync(
        bus.send(newState) *>
          latch.release.whenA(newState == State.RUNNING) *>
          stop.complete(Left(KafkaStreamsStoppedException())).whenA(newState == State.NOT_RUNNING))
  }

  private def kickoff(
    dispatcher: Dispatcher[F],
    err: Deferred[F, Either[Throwable, Unit]],
    stop: Deferred[F, Either[Throwable, Unit]],
    bus: Channel[F, State]): Resource[F, KafkaStreams] =
    Resource
      .make(F.blocking(new KafkaStreams(topology, settings.javaProperties)))(ks =>
        F.blocking(ks.close()) >> F.blocking(ks.cleanUp()))
      .evalMap { ks =>
        val start: F[KafkaStreams] = for {
          latch <- CountDownLatch[F](1)
          _ <- F.blocking(ks.cleanUp())
          _ <- F.blocking {
            ks.setUncaughtExceptionHandler(new StreamErrorHandler(dispatcher, err))
            ks.setStateListener(new StateChange(dispatcher, bus, latch, stop))
            ks.start()
          }
          _ <- latch.await
        } yield ks
        F.timeout(start, startUpTimeout)
      }

  val query: Stream[F, KafkaStreams] =
    for {
      dispatcher <- Stream.resource(Dispatcher[F])
      err <- Stream.eval(F.deferred[Either[Throwable, Unit]])
      stop <- Stream.eval(F.deferred[Either[Throwable, Unit]])
      bus <- Stream.eval(Channel.unbounded[F, State])
      ks <- Stream.resource(kickoff(dispatcher, err, stop, bus)).interruptWhen(err).interruptWhen(stop)
    } yield ks

  def stream: Stream[F, State] = for {
    dispatcher <- Stream.resource(Dispatcher[F])
    err <- Stream.eval(F.deferred[Either[Throwable, Unit]])
    stop <- Stream.eval(F.deferred[Either[Throwable, Unit]])
    bus <- Stream.eval(Channel.unbounded[F, State])
    _ <- Stream.resource(kickoff(dispatcher, err, stop, bus))
    state <- bus.stream.interruptWhen(err).interruptWhen(stop)
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

  def topology: Topology = {
    val builder: StreamsBuilder = new StreamsBuilder()
    val lss: StreamsBuilder     = localStateStores.foldLeft(builder)((bd, rd) => rd.run(bd))
    top.run(lss)
    builder.build()
  }
}

object KafkaStreamsBuilder {
  def apply[F[_]: Async](settings: KafkaStreamSettings, top: Reader[StreamsBuilder, Unit]): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](settings, top, Nil, 3.minutes)
}
