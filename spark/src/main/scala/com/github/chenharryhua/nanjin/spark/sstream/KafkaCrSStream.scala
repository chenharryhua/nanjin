package com.github.chenharryhua.nanjin.spark.sstream

import com.github.chenharryhua.nanjin.spark.DatePartitionedCR
import com.github.chenharryhua.nanjin.spark.kafka.OptionalKV
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

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
      typedDataset.deserialized.map {
        DatePartitionedCR(params.timeRange.zoneId)
      }.dataset.writeStream,
      cfg,
      path).partitionBy("Year", "Month", "Day")

  def sstream: SparkSStream[F, OptionalKV[K, V]] =
    new SparkSStream[F, OptionalKV[K, V]](ds, cfg)

  def toProducerRecords: KafkaPrSStream[F, K, V] =
    new KafkaPrSStream[F, K, V](typedDataset.deserialized.map(_.toNJProducerRecord).dataset, cfg)
}
