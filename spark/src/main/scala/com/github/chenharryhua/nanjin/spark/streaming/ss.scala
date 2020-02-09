package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.utils.Keyboard
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQueryProgress}

import scala.concurrent.duration._

trait NJStreamSink[F[_]] extends Serializable {
  def queryStream(implicit F: Concurrent[F], timer: Timer[F]): Stream[F, StreamingQueryProgress]

  final def run(implicit F: Concurrent[F], timer: Timer[F]): F[Unit] =
    queryStream.compile.drain

  final def showProgress(implicit F: Concurrent[F], timer: Timer[F]): F[Unit] =
    queryStream.mapFilter(Option(_).map(_.prettyJson)).showLinesStdOut.compile.drain
}

private[streaming] object ss {

  def queryStream[F[_], A](dsw: DataStreamWriter[A])(
    implicit F: Concurrent[F],
    timer: Timer[F]): Stream[F, StreamingQueryProgress] =
    for {
      kb <- Keyboard.signal[F]
      streamQuery <- Stream.eval(F.delay(dsw.start()))
      rst <- Stream
        .awakeEvery[F](5.second)
        .map(_ => streamQuery.exception.toLeft(()))
        .rethrow
        .interruptWhen(kb.map(_.filter(_ === Keyboard.Quit).map(_ => streamQuery.stop()).isDefined))
        .map(_ => streamQuery.lastProgress)
        .concurrently(Stream.eval(F.delay(streamQuery.awaitTermination())))
    } yield rst

}
