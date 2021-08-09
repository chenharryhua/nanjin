package com.github.chenharryhua.nanjin.spark.sstream

import cats.effect.kernel.Async
import cats.syntax.all.*
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQueryProgress}

import scala.concurrent.duration.*

private[sstream] object ss {

  def queryStream[F[_], A](dsw: DataStreamWriter[A], interval: FiniteDuration)(implicit
    F: Async[F]): Stream[F, StreamingQueryProgress] =
    for {
      streamQuery <- Stream.bracket(F.blocking(dsw.start()))(q => F.blocking(q.awaitTermination(3000)).void)
      rst <- Stream
        .awakeEvery[F](interval)
        .map(_ => streamQuery.exception.toLeft(()))
        .rethrow
        .map(_ => streamQuery.lastProgress)
    } yield rst
}
