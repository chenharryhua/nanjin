package com.github.chenharryhua.nanjin.spark.sstream

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final private[sstream] case class DatePartitionedCR[K, V](
  Year: String,
  Month: String,
  Day: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: Option[K],
  value: Option[V])

final class KafkaCrSStream[F[_], K: TypedEncoder, V: TypedEncoder](
  ds: Dataset[OptionalKV[K, V]],
  cfg: SStreamConfig)
    extends SparkStreamUpdateParams[KafkaCrSStream[F, K, V]] {

  override def withParamUpdate(f: SStreamConfig => SStreamConfig): KafkaCrSStream[F, K, V] =
    new KafkaCrSStream[F, K, V](ds, f(cfg))

  @transient lazy val typedDataset: TypedDataset[OptionalKV[K, V]] = TypedDataset.create(ds)

  override val params: SStreamParams = cfg.evalConfig

  def someValues: KafkaCrSStream[F, K, V] =
    new KafkaCrSStream[F, K, V](typedDataset.filter(typedDataset('value).isNotNone).dataset, cfg)

  def datePartitionFileSink(path: String): NJFileSink[F, DatePartitionedCR[K, V]] =
    new NJFileSink[F, DatePartitionedCR[K, V]](
      typedDataset.deserialized.map { m =>
        val time = NJTimestamp(m.timestamp)
        val tz   = params.timeRange.zoneId
        DatePartitionedCR(
          time.yearStr(tz),
          time.monthStr(tz),
          time.dayStr(tz),
          m.partition,
          m.offset,
          m.timestamp,
          m.key,
          m.value)
      }.dataset.writeStream,
      cfg,
      path).partitionBy("Year", "Month", "Day")

  def sstream: SparkSStream[F, OptionalKV[K, V]] =
    new SparkSStream[F, OptionalKV[K, V]](ds, cfg)

  def toProducerRecords: KafkaPrSStream[F, K, V] =
    new KafkaPrSStream[F, K, V](typedDataset.deserialized.map(_.toNJProducerRecord).dataset, cfg)
}
