package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.kernel.Resource.ExitCase
import cats.effect.kernel.{Async, Deferred}
import cats.effect.std.Dispatcher
import cats.syntax.all._
import fs2.Stream
import monocle.function.At.at
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.{KafkaStreams, Topology}

final case class UncaughtKafkaStreamingException(thread: Thread, ex: Throwable) extends Exception(ex.getMessage)

final case class KafkaStartupException() extends Exception("failed to start kafka streaming")

final class KafkaStreamsBuilder[F[_]](
  settings: KafkaStreamSettings,
  top: Reader[StreamsBuilder, Unit],
  localStateStores: List[Reader[StreamsBuilder, StreamsBuilder]]) {

  final private class StreamErrorHandler(
    deferred: Deferred[F, UncaughtKafkaStreamingException],
    dispatcher: Dispatcher[F])(implicit F: Async[F])
      extends Thread.UncaughtExceptionHandler {

    override def uncaughtException(t: Thread, e: Throwable): Unit =
      dispatcher.unsafeRunSync(deferred.complete(UncaughtKafkaStreamingException(t, e)).void)
  }

  final private class Latch(value: Deferred[F, Either[KafkaStartupException, Unit]], dispatcher: Dispatcher[F])(implicit
    F: Async[F])
      extends KafkaStreams.StateListener {

    override def onChange(newState: KafkaStreams.State, oldState: KafkaStreams.State): Unit =
      newState match {
        case KafkaStreams.State.RUNNING => dispatcher.unsafeRunSync(value.complete(Right(())).attempt.void)
        case KafkaStreams.State.ERROR =>
          dispatcher.unsafeRunSync(value.complete(Left(KafkaStartupException())).attempt.void)
        case _ => ()
      }
  }

  def run(implicit F: Async[F]): Stream[F, KafkaStreams] =
    for {
      errorListener <- Stream.eval(Deferred[F, UncaughtKafkaStreamingException])
      latch <- Stream.eval(Deferred[F, Either[KafkaStartupException, Unit]])
      dispatcher <- Stream.resource(Dispatcher[F])
      kss <-
        Stream
          .bracketCase(F.delay(new KafkaStreams(topology, settings.javaProperties))) { case (ks, reason) =>
            reason match {
              case ExitCase.Succeeded  => F.blocking(ks.close())
              case ExitCase.Canceled   => F.blocking(ks.close()) >> F.blocking(ks.cleanUp())
              case ExitCase.Errored(_) => F.blocking(ks.close()) >> F.blocking(ks.cleanUp())
            }
          }
          .evalMap(ks =>
            F.delay {
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
