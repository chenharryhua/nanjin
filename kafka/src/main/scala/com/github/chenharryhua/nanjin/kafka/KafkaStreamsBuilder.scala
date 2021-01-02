package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.ConcurrentEffect
import cats.effect.concurrent.Deferred
import cats.syntax.functor._
import cats.syntax.monadError._
import fs2.Stream
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, Topology}

final case class UncaughtKafkaStreamingException(thread: Thread, ex: Throwable) extends Exception(ex.getMessage)

final case class KafkaStreamingStartupException() extends Exception("failed to start kafka streaming")

final class KafkaStreamsBuilder[F[_]](
  builder: StreamsBuilder,
  settings: KafkaStreamSettings,
  top: Reader[StreamsBuilder, Unit]) {

  final private class StreamErrorHandler(deferred: Deferred[F, UncaughtKafkaStreamingException], F: ConcurrentEffect[F])
      extends Thread.UncaughtExceptionHandler {

    override def uncaughtException(t: Thread, e: Throwable): Unit =
      F.toIO(deferred.complete(UncaughtKafkaStreamingException(t, e))).void.unsafeRunSync()
  }

  final private class Latch(value: Deferred[F, Either[KafkaStreamingStartupException, Unit]], F: ConcurrentEffect[F])
      extends KafkaStreams.StateListener {

    override def onChange(newState: KafkaStreams.State, oldState: KafkaStreams.State): Unit =
      newState match {
        case KafkaStreams.State.RUNNING =>
          F.toIO(value.complete(Right(()))).attempt.void.unsafeRunSync()
        case KafkaStreams.State.ERROR =>
          F.toIO(value.complete(Left(KafkaStreamingStartupException()))).attempt.void.unsafeRunSync()
        case _ => ()
      }
  }

  def run(implicit F: ConcurrentEffect[F]): Stream[F, KafkaStreams] =
    for {
      errorListener <- Stream.eval(Deferred[F, UncaughtKafkaStreamingException])
      latch <- Stream.eval(Deferred[F, Either[KafkaStreamingStartupException, Unit]])
      kss <-
        Stream
          .bracket(F.delay(new KafkaStreams(topology, settings.javaProperties)))(ks =>
            F.delay { ks.close(); ks.cleanUp() })
          .evalMap(ks =>
            F.delay {
              ks.setUncaughtExceptionHandler(new StreamErrorHandler(errorListener, F))
              ks.setStateListener(new Latch(latch, F))
              ks.start()
            }.as(ks))
          .concurrently(Stream.eval(errorListener.get).flatMap(Stream.raiseError[F]))
      _ <- Stream.eval(latch.get.rethrow)
    } yield kss

  def topology: Topology = {
    top.run(builder)
    builder.build()
  }
}
