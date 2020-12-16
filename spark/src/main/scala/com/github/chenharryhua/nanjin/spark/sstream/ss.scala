package com.github.chenharryhua.nanjin.spark.sstream

import cats.effect.{Concurrent, Timer}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.utils.Keyboard
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQueryProgress}

import scala.concurrent.duration._

trait NJStreamSink[F[_]] extends Serializable {
  def params: SStreamParams

  def queryStream(implicit F: Concurrent[F], timer: Timer[F]): Stream[F, StreamingQueryProgress]

  final def showProgress(implicit F: Concurrent[F], timer: Timer[F]): Stream[F, Unit] =
    queryStream.mapFilter(Option(_).map(_.prettyJson)).showLinesStdOut
}

private[sstream] object ss {

  def queryStream[F[_], A](dsw: DataStreamWriter[A], interval: FiniteDuration)(implicit
    F: Concurrent[F],
    timer: Timer[F]): Stream[F, StreamingQueryProgress] =
    for {
      kb <- Keyboard.signal[F]
      streamQuery <- Stream.bracket(F.delay(dsw.start()))(q => F.delay(q.stop()))
      rst <- Stream
        .awakeEvery[F](interval)
        .map(_ => streamQuery.exception.toLeft(()))
        .rethrow
        .interruptWhen(kb.map(_.contains(Keyboard.Quit)))
        .map(_ => streamQuery.lastProgress)
    } yield rst
}
