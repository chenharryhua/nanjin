package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQueryProgress}

final case class PartitionedConsumerRecord[K, V](
  topicName: String,
  Year: String,
  Month: String,
  Day: String,
  payload: NJConsumerRecord[K, V])

final class NJConsumerRecordPartitionFileSink[F[_], K, V](
  dsw: DataStreamWriter[NJConsumerRecord[K, V]],
  params: StreamConfigF.StreamConfig,
  path: String)
    extends NJStreamSink[F] {

  private val p: StreamParams = StreamConfigF.evalParams(params)

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
