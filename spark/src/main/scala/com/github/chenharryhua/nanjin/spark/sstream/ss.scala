package com.github.chenharryhua.nanjin.spark.sstream

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQueryProgress}

import scala.concurrent.duration._

trait NJStreamSink[F[_]] extends Serializable {
  def params: SStreamParams

  def queryStream(implicit F: Async[F]): Stream[F, StreamingQueryProgress]

  final def showProgress(implicit F: Async[F]): Stream[F, String] =
    queryStream.mapFilter(Option(_).map(_.prettyJson)).debug()
}

private[sstream] object ss {

  def queryStream[F[_], A](dsw: DataStreamWriter[A], interval: FiniteDuration)(implicit
    F: Async[F]): Stream[F, StreamingQueryProgress] =
    for {
      streamQuery <- Stream.bracket(F.delay(dsw.start()))(q => F.delay(q.stop()))
      rst <- Stream
        .awakeEvery[F](interval)
        .map(_ => streamQuery.exception.toLeft(()))
        .rethrow
        .map(_ => streamQuery.lastProgress)
    } yield rst
}
