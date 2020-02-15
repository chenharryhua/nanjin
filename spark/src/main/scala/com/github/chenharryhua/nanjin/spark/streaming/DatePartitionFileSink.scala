package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQueryProgress}

final case class DatePartitioned[K, V](
  Year: String,
  Month: String,
  Day: String,
  partition: Int,
  offset: Long,
  key: Option[K],
  value: Option[V])

final class DatePartitionFileSink[F[_], K, V](
  dsw: DataStreamWriter[DatePartitioned[K, V]],
  cfg: StreamConfig,
  path: String)
    extends NJStreamSink[F] {

  private val p: StreamParams = StreamConfigF.evalConfig(cfg)

  override def queryStream(
    implicit F: Concurrent[F],
    timer: Timer[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream(
      dsw
        .partitionBy("Year", "Month", "Day")
        .trigger(p.trigger)
        .format(p.fileFormat.format)
        .outputMode(OutputMode.Append)
        .option("path", path)
        .option("checkpointLocation", p.checkpoint.value)
        .option("failOnDataLoss", p.dataLoss.value))

}
