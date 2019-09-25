package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.concurrent.Deferred
import cats.effect.{ConcurrentEffect, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.codec.UncaughtKafkaStreamingException
import fs2.Stream
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder

final class KafkaStreamRunner[F[_]](settings: KafkaStreamSettings)(
  implicit F: ConcurrentEffect[F],
  timer: Timer[F]) {

  final private class Handler(deferred: Deferred[F, UncaughtKafkaStreamingException])
      extends Thread.UncaughtExceptionHandler {
    override def uncaughtException(t: Thread, e: Throwable): Unit =
      F.toIO(deferred.complete(UncaughtKafkaStreamingException(t, e))).void.unsafeRunSync()
  }

  final private class Latch(latch: Deferred[F, Unit]) extends KafkaStreams.StateListener {
    override def onChange(newState: KafkaStreams.State, oldState: KafkaStreams.State): Unit =
      newState match {
        case KafkaStreams.State.RUNNING => F.toIO(latch.complete(())).void.unsafeRunSync()
        case _                          => ()
      }
  }

  def stream(topology: Reader[StreamsBuilder, Unit]): Stream[F, KafkaStreams] =
    for {
      error <- Stream.eval(Deferred[F, UncaughtKafkaStreamingException])
      latch <- Stream.eval(Deferred[F, Unit])
      kss <- Stream
        .bracket(F.delay {
          val builder: StreamsBuilder = new StreamsBuilder
          topology.run(builder)
          new KafkaStreams(builder.build(), settings.settings)
        })(ks => F.delay(ks.close()))
        .evalMap(ks =>
          F.delay {
            ks.cleanUp()
            ks.setUncaughtExceptionHandler(new Handler(error))
            ks.setStateListener(new Latch(latch))
            ks.start()
          }.as(ks))
        .concurrently(Stream.eval(error.get).flatMap(Stream.raiseError[F]))
      _ <- Stream.eval(latch.get)
    } yield kss
}
