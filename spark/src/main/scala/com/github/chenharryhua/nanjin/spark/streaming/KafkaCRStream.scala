package com.github.chenharryhua.nanjin.spark.streaming

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class KafkaCRStream[F[_], K: TypedEncoder, V: TypedEncoder](
  ds: Dataset[NJConsumerRecord[K, V]],
  params: StreamConfigF.StreamConfig)
    extends Serializable {

  @transient lazy val typedDataset: TypedDataset[NJConsumerRecord[K, V]] = TypedDataset.create(ds)

  private val p: StreamParams = StreamConfigF.evalParams(params)

  def partitionedFileSink(path: String): NJConsumerRecordPartitionFileSink[F, K, V] =
    new NJConsumerRecordPartitionFileSink[F, K, V](typedDataset.deserialized.map { m =>
      val time = NJTimestamp(m.timestamp / 1000)
      val tz   = p.timeRange.zoneId
      PartitionedConsumerRecord(m.topic, time.yearStr(tz), time.monthStr(tz), time.dayStr(tz), m)
    }.dataset.writeStream, params, path)

}
