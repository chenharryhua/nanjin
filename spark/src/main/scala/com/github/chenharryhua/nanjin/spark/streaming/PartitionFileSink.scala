package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQueryProgress}

final case class PartitionedConsumerRecord[K, V](
  Topic: String,
  Year: String,
  Month: String,
  Day: String,
  payload: NJConsumerRecord[K, V])

final class PartitionFileSink[F[_], K, V](
  dsw: DataStreamWriter[PartitionedConsumerRecord[K, V]],
  cfg: StreamConfig,
  path: String)
    extends NJStreamSink[F] {

  private val p: StreamParams = StreamConfigF.evalParams(cfg)

  override def queryStream(
    implicit F: Concurrent[F],
    timer: Timer[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream(
      dsw
        .partitionBy("Topic", "Year", "Month", "Day")
        .trigger(p.trigger)
        .format(p.fileFormat.format)
        .outputMode(OutputMode.Append)
        .option("path", path)
        .option("checkpointLocation", p.checkpoint.value)
        .option("failOnDataLoss", p.dataLoss.value))

}
