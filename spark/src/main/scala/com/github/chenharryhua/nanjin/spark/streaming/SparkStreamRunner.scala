package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import cats.implicits._
import org.apache.spark.sql.streaming.DataStreamWriter

final class SparkStreamRunner[F[_], A](dsw: DataStreamWriter[A], sink: StreamOutputSink)
    extends Serializable {

  def withOptions(f: DataStreamWriter[A] => DataStreamWriter[A]) =
    new SparkStreamRunner[F, A](f(dsw), sink)

  def partitionBy(colNames: String*): SparkStreamRunner[F, A] =
    new SparkStreamRunner[F, A](dsw.partitionBy(colNames: _*), sink)

  def run(implicit F: Concurrent[F], timer: Timer[F]): F[Unit] =
    ss.queryStream(sink.sinkOptions(dsw)).compile.drain

}
