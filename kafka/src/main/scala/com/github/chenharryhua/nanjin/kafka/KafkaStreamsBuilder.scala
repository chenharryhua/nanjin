package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.kernel.{Async, Deferred, Resource}
import cats.effect.std.{CountDownLatch, Dispatcher}
import cats.syntax.all.*
import fs2.Stream
import monocle.function.At.at
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.{KafkaStreams, Topology}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

final case class KafkaStreamsException(msg: String) extends Exception(msg)

final class KafkaStreamsBuilder[F[_]] private (
  settings: KafkaStreamSettings,
  top: Reader[StreamsBuilder, Unit],
  localStateStores: List[Reader[StreamsBuilder, StreamsBuilder]],
  startUpTimeout: FiniteDuration)(implicit F: Async[F]) {
  final private class StreamErrorHandler(dispatcher: Dispatcher[F], errorListener: Deferred[F, KafkaStreamsException])
      extends StreamsUncaughtExceptionHandler {

    override def handle(throwable: Throwable): StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse = {
      dispatcher.unsafeRunSync(errorListener.complete(KafkaStreamsException(ExceptionUtils.getMessage(throwable))))
      StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION
    }
  }

  final private class StateChange(
    dispatcher: Dispatcher[F],
    stopSignal: Deferred[F, KafkaStreamsException],
    latch: CountDownLatch[F])
      extends KafkaStreams.StateListener {

    override def onChange(newState: State, oldState: State): Unit =
      dispatcher.unsafeRunSync(
        latch.release.whenA(newState == State.RUNNING) >>
          stopSignal.complete(KafkaStreamsException("Kafka streams were stopped")).whenA(newState == State.NOT_RUNNING))
  }

  private def kickoff(
    dispatcher: Dispatcher[F],
    errorListener: Deferred[F, KafkaStreamsException],
    stopSignal: Deferred[F, KafkaStreamsException]): Resource[F, KafkaStreams] =
    Resource
      .make(F.blocking(new KafkaStreams(topology, settings.javaProperties)))(ks =>
        F.blocking(ks.close()) >> F.blocking(ks.cleanUp()))
      .evalMap { ks =>
        val start: F[KafkaStreams] = for {
          latch <- CountDownLatch[F](1)
          _ <- F.blocking(ks.cleanUp())
          _ <- F.blocking {
            ks.setUncaughtExceptionHandler(new StreamErrorHandler(dispatcher, errorListener))
            ks.setStateListener(new StateChange(dispatcher, stopSignal, latch))
            ks.start()
          }
          _ <- latch.await
        } yield ks
        F.timeout(start, startUpTimeout)
      }

  private val resource: Resource[F, (KafkaStreams, F[KafkaStreamsException])] =
    for {
      dispatcher <- Dispatcher[F]
      errorListener <- Resource.eval(F.deferred[KafkaStreamsException])
      stopSignal <- Resource.eval(F.deferred[KafkaStreamsException])
      ks <- kickoff(dispatcher, errorListener, stopSignal)
    } yield (ks, F.race(errorListener.get, stopSignal.get).map(_.fold(identity, identity)))

  def stream: Stream[F, Nothing] =
    Stream.resource(resource).evalMap(_._2.flatMap[Nothing](F.raiseError))

  def query: Stream[F, KafkaStreams] =
    Stream.resource(resource).flatMap { case (ks, err) =>
      Stream(ks).concurrently(Stream.eval(err.flatMap[Nothing](F.raiseError)))
    }

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
