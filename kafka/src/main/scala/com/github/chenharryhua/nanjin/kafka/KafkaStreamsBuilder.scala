package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.std.Dispatcher
import cats.effect.{Async, Deferred}
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.monadError._
import fs2.Stream
import monocle.function.At.at
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.{KafkaStreams, Topology}

final case class UncaughtKafkaStreamingException(ex: Throwable) extends Exception(ex.getMessage)

final case class KafkaStreamingStartupException() extends Exception("failed to start kafka streaming")

final class KafkaStreamsBuilder[F[_]](
  settings: KafkaStreamSettings,
  top: Reader[StreamsBuilder, Unit],
  localStateStores: List[Reader[StreamsBuilder, StreamsBuilder]]) {

  final private class StreamErrorHandler(deferred: Deferred[F, UncaughtKafkaStreamingException], F: Dispatcher[F])
      extends StreamsUncaughtExceptionHandler {

    override def handle(throwable: Throwable): StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse = {
      F.unsafeRunAndForget(deferred.complete(UncaughtKafkaStreamingException(throwable)))
      StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION
    }
  }

  final private class Latch(value: Deferred[F, Either[KafkaStreamingStartupException, Unit]], F: Dispatcher[F])
      extends KafkaStreams.StateListener {

    override def onChange(newState: KafkaStreams.State, oldState: KafkaStreams.State): Unit =
      newState match {
        case KafkaStreams.State.RUNNING =>
          F.unsafeRunAndForget(value.complete(Right(())))
        case KafkaStreams.State.ERROR =>
          F.unsafeRunAndForget(value.complete(Left(KafkaStreamingStartupException())))
        case _ => ()
      }
  }

  def run(implicit F: Async[F]): Stream[F, KafkaStreams] =
    for {
      dispatcher <- Stream.resource(Dispatcher[F])
      errorListener <- Stream.eval(Deferred[F, UncaughtKafkaStreamingException])
      latch <- Stream.eval(Deferred[F, Either[KafkaStreamingStartupException, Unit]])
      kss <-
        Stream
          .bracket(F.blocking(new KafkaStreams(topology, settings.javaProperties)))(ks =>
            F.blocking(ks.close()) >> F.blocking(ks.cleanUp()))
          .evalMap(ks =>
            F.blocking {
              ks.setUncaughtExceptionHandler(new StreamErrorHandler(errorListener, dispatcher))
              ks.setStateListener(new Latch(latch, dispatcher))
              ks.start()
            }.as(ks))
          .concurrently(Stream.eval(errorListener.get).flatMap(Stream.raiseError[F]))
      _ <- Stream.eval(latch.get.rethrow)
    } yield kss

  def withProperty(key: String, value: String): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      KafkaStreamSettings.config.composeLens(at(key)).set(Some(value))(settings),
      top,
      localStateStores)

  def addStateStore[S <: StateStore](storeBuilder: StoreBuilder[S]): KafkaStreamsBuilder[F] =
    new KafkaStreamsBuilder[F](
      settings,
      top,
      Reader((sb: StreamsBuilder) => new StreamsBuilder(sb.addStateStore(storeBuilder))) :: localStateStores)

  def topology: Topology = {
    val builder: StreamsBuilder = new StreamsBuilder()
    val lss: StreamsBuilder     = localStateStores.foldLeft(builder)((bd, rd) => rd.run(bd))
    top.run(lss)
    builder.build()
  }
}
