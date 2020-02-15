package com.github.chenharryhua.nanjin.spark.streaming

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class KafkaCRStream[F[_], K: TypedEncoder, V: TypedEncoder](
  ds: Dataset[NJConsumerRecord[K, V]],
  cfg: StreamConfig)
    extends SparkStreamUpdateParams[KafkaCRStream[F, K, V]] {

  override def withParamUpdate(f: StreamConfig => StreamConfig): KafkaCRStream[F, K, V] =
    new KafkaCRStream[F, K, V](ds, f(cfg))

  @transient lazy val typedDataset: TypedDataset[NJConsumerRecord[K, V]] = TypedDataset.create(ds)

  private val p: StreamParams = StreamConfigF.evalConfig(cfg)

  def datePartitionFileSink(path: String): DatePartitionFileSink[F, K, V] =
    new DatePartitionFileSink[F, K, V](typedDataset.deserialized.map { m =>
      val time = NJTimestamp(m.timestamp / 1000)
      val tz   = p.timeRange.zoneId
      DatePartitionedCR(time.yearStr(tz), time.monthStr(tz), time.dayStr(tz), m)
    }.dataset.writeStream, cfg, path)

  def consoleSink: NJConsoleSink[F, NJConsumerRecord[K, V]] =
    new SparkStream[F, NJConsumerRecord[K, V]](ds, cfg).consoleSink

  def fileSink(path: String): NJFileSink[F, NJConsumerRecord[K, V]] =
    new SparkStream[F, NJConsumerRecord[K, V]](ds, cfg).fileSink(path)

  def toPRStream: KafkaPRStream[F, K, V] =
    new KafkaPRStream[F, K, V](typedDataset.deserialized.map(_.toNJProducerRecord).dataset, cfg)
}
