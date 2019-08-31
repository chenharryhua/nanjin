package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import cats.effect.ConcurrentEffect
import cats.effect.concurrent.Deferred
import cats.implicits._
import com.github.chenharryhua.nanjin.codec.UncaughtKafkaStreamingException
import fs2.Stream
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder

final class KafkaStreamRunner[F[_]](settings: KafkaStreamSettings)(
  implicit F: ConcurrentEffect[F]) {

  final private class Handler(deferred: Deferred[F, UncaughtKafkaStreamingException])
      extends Thread.UncaughtExceptionHandler {
    override def uncaughtException(t: Thread, e: Throwable): Unit =
      F.toIO(deferred.complete(UncaughtKafkaStreamingException(t, e))).void.unsafeRunSync()
  }

  def stream[A](topology: Reader[StreamsBuilder, A]): Stream[F, KafkaStreams] =
    Stream.eval(Deferred[F, UncaughtKafkaStreamingException]).flatMap { df =>
      Stream
        .bracket(F.delay {
          val builder: StreamsBuilder = new StreamsBuilder
          topology.run(builder)
          new KafkaStreams(builder.build(), settings.settings)
        })(r => F.delay(r.close()))
        .evalMap(ks =>
          F.delay {
            ks.cleanUp()
            ks.setUncaughtExceptionHandler(new Handler(df))
            ks.start()
          }.as(ks))
        .concurrently(Stream.eval(df.get).flatMap(Stream.raiseError[F]))
    }
}
