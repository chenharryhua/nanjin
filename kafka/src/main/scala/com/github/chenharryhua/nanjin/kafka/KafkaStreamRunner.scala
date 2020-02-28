package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.ConcurrentEffect
import cats.effect.concurrent.Deferred
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.codec.{
  KafkaStreamingException,
  UncaughtKafkaStreamingException
}
import com.github.chenharryhua.nanjin.utils.Keyboard
import fs2.Stream
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder

final class KafkaStreamRunner[F[_]](settings: KafkaStreamSettings)(
  implicit F: ConcurrentEffect[F]) {

  final private class StreamErrorHandler(deferred: Deferred[F, UncaughtKafkaStreamingException])
      extends Thread.UncaughtExceptionHandler {

    override def uncaughtException(t: Thread, e: Throwable): Unit =
      F.toIO(deferred.complete(UncaughtKafkaStreamingException(t, e))).void.unsafeRunSync()
  }

  final private class Latch(value: Deferred[F, Either[KafkaStreamingException, Unit]])
      extends KafkaStreams.StateListener {

    override def onChange(newState: KafkaStreams.State, oldState: KafkaStreams.State): Unit =
      newState match {
        case KafkaStreams.State.RUNNING =>
          F.toIO(value.complete(Right(()))).attempt.void.unsafeRunSync()
        case KafkaStreams.State.ERROR =>
          F.toIO(value.complete(Left(KafkaStreamingException("kafka streaming failure"))))
            .attempt
            .void
            .unsafeRunSync()
        case _ => ()
      }
  }

  def stream(topology: Reader[StreamsBuilder, Unit]): Stream[F, KafkaStreams] =
    for {
      kb <- Keyboard.signal[F]
      eh <- Stream.eval(Deferred[F, UncaughtKafkaStreamingException])
      latch <- Stream.eval(Deferred[F, Either[KafkaStreamingException, Unit]])
      kss <- Stream
        .bracket(F.delay {
          val builder: StreamsBuilder = new StreamsBuilder
          topology.run(builder)
          new KafkaStreams(builder.build(), settings.javaProperties)
        })(ks => F.delay(ks.close()))
        .evalMap(ks =>
          F.delay {
            ks.cleanUp()
            ks.setUncaughtExceptionHandler(new StreamErrorHandler(eh))
            ks.setStateListener(new Latch(latch))
            ks.start()
          }.as(ks))
        .interruptWhen(kb.map(_.contains(Keyboard.Quit)))
        .concurrently(Stream.eval(eh.get).flatMap(Stream.raiseError[F]))
      _ <- Stream.eval(latch.get.rethrow)
    } yield kss
}
