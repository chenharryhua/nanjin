package com.github.chenharryhua.nanjin.spark.sstream

import com.github.chenharryhua.nanjin.spark.DatePartitionedCR
import com.github.chenharryhua.nanjin.spark.kafka.OptionalKV
import frameless.{TypedDataset, TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.{Dataset, Encoder}

final class KafkaCrSStream[F[_], K: TypedEncoder, V: TypedEncoder](
  ds: Dataset[OptionalKV[K, V]],
  cfg: SStreamConfig)
    extends SparkStreamUpdateParams[KafkaCrSStream[F, K, V]] {

  override def withParamUpdate(f: SStreamConfig => SStreamConfig): KafkaCrSStream[F, K, V] =
    new KafkaCrSStream[F, K, V](ds, f(cfg))

  implicit private val te: TypedEncoder[OptionalKV[K, V]] = shapeless.cachedImplicit

  @transient lazy val typedDataset: TypedDataset[OptionalKV[K, V]] = TypedDataset.create(ds)

  override val params: SStreamParams = cfg.evalConfig

  def map[K1, V1](f: OptionalKV[K, V] => OptionalKV[K1, V1])(implicit
    k1: TypedEncoder[K1],
    v1: TypedEncoder[V1]): KafkaCrSStream[F, K1, V1] = {
    implicit val te: TypedEncoder[OptionalKV[K1, V1]] = shapeless.cachedImplicit
    val encoder: Encoder[OptionalKV[K1, V1]]          = TypedExpressionEncoder[OptionalKV[K1, V1]]
    new KafkaCrSStream[F, K1, V1](ds.map(f)(encoder), cfg)
  }

  def flatMap[K1, V1](f: OptionalKV[K, V] => TraversableOnce[OptionalKV[K1, V1]])(implicit
    k1: TypedEncoder[K1],
    v1: TypedEncoder[V1]): KafkaCrSStream[F, K1, V1] = {
    implicit val te: TypedEncoder[OptionalKV[K1, V1]] = shapeless.cachedImplicit
    val encoder: Encoder[OptionalKV[K1, V1]]          = TypedExpressionEncoder[OptionalKV[K1, V1]]
    new KafkaCrSStream[F, K1, V1](ds.flatMap(f)(encoder), cfg)
  }

  def someValues: KafkaCrSStream[F, K, V] =
    new KafkaCrSStream[F, K, V](typedDataset.filter(typedDataset('value).isNotNone).dataset, cfg)

  def datePartitionFileSink(path: String): NJFileSink[F, DatePartitionedCR[K, V]] = {
    implicit val te: TypedEncoder[DatePartitionedCR[K, V]] = shapeless.cachedImplicit
    new NJFileSink[F, DatePartitionedCR[K, V]](
      typedDataset.deserialized.map {
        DatePartitionedCR(params.timeRange.zoneId)
      }.dataset.writeStream,
      cfg,
      path).partitionBy("Year", "Month", "Day")
  }

  def sstream: SparkSStream[F, OptionalKV[K, V]] =
    new SparkSStream[F, OptionalKV[K, V]](ds, cfg)

  def toProducerRecords: KafkaPrSStream[F, K, V] =
    new KafkaPrSStream[F, K, V](typedDataset.deserialized.map(_.toNJProducerRecord).dataset, cfg)
}
