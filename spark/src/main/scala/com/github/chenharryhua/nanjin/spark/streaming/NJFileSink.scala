package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQueryProgress}

final class NJFileSink[F[_], A](dsw: DataStreamWriter[A], cfg: StreamConfig, path: String)
    extends NJStreamSink[F] {

  private val p: StreamParams = StreamConfigF.evalConfig(cfg)

  // ops
  def withOptions(f: DataStreamWriter[A] => DataStreamWriter[A]): NJFileSink[F, A] =
    new NJFileSink(f(dsw), cfg, path)

  def partitionBy(colNames: String*): NJFileSink[F, A] =
    new NJFileSink[F, A](dsw.partitionBy(colNames: _*), cfg, path)

  override def queryStream(
    implicit F: Concurrent[F],
    timer: Timer[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream(
      dsw
        .trigger(p.trigger)
        .format(p.fileFormat.format)
        .outputMode(OutputMode.Append)
        .option("path", path)
        .option("checkpointLocation", p.checkpoint.value)
        .option("failOnDataLoss", p.dataLoss.value))

}
