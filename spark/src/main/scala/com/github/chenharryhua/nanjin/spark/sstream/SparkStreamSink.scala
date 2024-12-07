package com.github.chenharryhua.nanjin.spark.sstream

import cats.effect.kernel.Async
import cats.syntax.all.*
import fs2.Stream
import org.apache.spark.sql.streaming.StreamingQueryProgress

trait SparkStreamSink[F[_]] extends Serializable {
  def params: SStreamParams

  def stream(implicit F: Async[F]): Stream[F, StreamingQueryProgress]

  final def showProgress(implicit F: Async[F]): Stream[F, String] =
    stream.mapFilter(Option(_).map(_.prettyJson)).debug()
}
