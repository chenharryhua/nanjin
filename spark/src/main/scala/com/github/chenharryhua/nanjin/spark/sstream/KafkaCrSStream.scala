package com.github.chenharryhua.nanjin.spark.sstream

import com.github.chenharryhua.nanjin.spark.kafka.OptionalKV
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.{Dataset, Encoder}

final class KafkaCrSStream[F[_], K, V](ds: Dataset[OptionalKV[K, V]], cfg: SStreamConfig)
    extends Serializable {

  def withParamUpdate(f: SStreamConfig => SStreamConfig): KafkaCrSStream[F, K, V] =
    new KafkaCrSStream[F, K, V](ds, f(cfg))

  val params: SStreamParams = cfg.evalConfig

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

  def datePartitionFileSink(path: String)(implicit
    k: TypedEncoder[K],
    v: TypedEncoder[V]): NJFileSink[F, DatePartitionedCR[K, V]] = {
    implicit val te: TypedEncoder[DatePartitionedCR[K, V]] = shapeless.cachedImplicit
    val enc: Encoder[DatePartitionedCR[K, V]]              = TypedExpressionEncoder(te)
    new NJFileSink[F, DatePartitionedCR[K, V]](
      ds.map(DatePartitionedCR(params.timeRange.zoneId))(enc).writeStream,
      cfg,
      path).partitionBy("Year", "Month", "Day")
  }

  def sstream: SparkSStream[F, OptionalKV[K, V]] =
    new SparkSStream[F, OptionalKV[K, V]](ds, cfg)

}
