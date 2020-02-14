package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQueryProgress}

final class NJFileSink[F[_], A](
  dsw: DataStreamWriter[A],
  params: StreamConfigF.StreamConfig,
  path: String)
    extends NJStreamSink[F] {

  private val p: StreamParams = StreamConfigF.evalParams(params)

  // ops
  def withOptions(f: DataStreamWriter[A] => DataStreamWriter[A]): NJFileSink[F, A] =
    new NJFileSink(f(dsw), params, path)

  def partitionBy(colNames: String*): NJFileSink[F, A] =
    new NJFileSink[F, A](dsw.partitionBy(colNames: _*), params, path)

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
