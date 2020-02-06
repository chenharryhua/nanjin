package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.utils.Keyboard
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQueryProgress}

import scala.concurrent.duration._

final class SparkStreamRunner[F[_], A](dsw: DataStreamWriter[A], sink: StreamOutputSink)
    extends Serializable {

  def withOptions(f: DataStreamWriter[A] => DataStreamWriter[A]) =
    new SparkStreamRunner[F, A](f(dsw), sink)

  def partitionBy(colNames: String*): SparkStreamRunner[F, A] =
    new SparkStreamRunner[F, A](dsw.partitionBy(colNames: _*), sink)

  def queryStream(implicit F: Concurrent[F], timer: Timer[F]): Stream[F, StreamingQueryProgress] =
    for {
      kb <- Keyboard.signal[F]
      streamQuery <- Stream.eval(F.delay(sink.sinkOptions(dsw).start()))
      rst <- Stream
        .awakeEvery[F](5.second)
        .map(_ => streamQuery.exception.toLeft(()))
        .rethrow
        .interruptWhen(kb.map(_.filter(_ === Keyboard.Quit).map(_ => streamQuery.stop()).isDefined))
        .map(_ => streamQuery.lastProgress)
        .concurrently(Stream.eval(F.delay(streamQuery.awaitTermination())))
    } yield rst

  def run(implicit F: Concurrent[F], timer: Timer[F]): F[Unit] =
    queryStream.compile.drain

}
