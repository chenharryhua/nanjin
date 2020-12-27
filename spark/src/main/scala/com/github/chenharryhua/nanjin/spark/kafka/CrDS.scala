package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import cats.syntax.bifunctor._
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.DatasetAvroFileHoarder
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.{TypedDataset, TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

final class CrDS[F[_], K, V] private[kafka] (
  val topic: KafkaTopic[F, K, V],
  val dataset: Dataset[OptionalKV[K, V]],
  val ate: AvroTypedEncoder[OptionalKV[K, V]],
  val cfg: SKConfig)
    extends Serializable {

  val params: SKParams = cfg.evalConfig

  private def updateCfg(f: SKConfig => SKConfig): CrDS[F, K, V] = new CrDS[F, K, V](topic, dataset, ate, f(cfg))

  def typedDataset: TypedDataset[OptionalKV[K, V]] = TypedDataset.create(dataset)(ate.typedEncoder)

  def transform(f: Dataset[OptionalKV[K, V]] => Dataset[OptionalKV[K, V]]): CrDS[F, K, V] =
    new CrDS[F, K, V](topic, f(dataset), ate, cfg)

  def partitionOf(num: Int): CrDS[F, K, V] = transform(_.filter(col("partition") === num))

  def offsetRange(start: Long, end: Long): CrDS[F, K, V] = transform(range.offset(start, end))
  def timeRange(dr: NJDateTimeRange): CrDS[F, K, V]      = transform(range.timestamp(dr))
  def timeRange: CrDS[F, K, V]                           = timeRange(params.timeRange)

  def ascendOffset: CrDS[F, K, V]     = transform(sort.ascend.offset).updateCfg(_.withSorted)
  def descendOffset: CrDS[F, K, V]    = transform(sort.descend.offset).updateCfg(_.withSorted)
  def ascendTimestamp: CrDS[F, K, V]  = transform(sort.ascend.timestamp).updateCfg(_.withSorted)
  def descendTimestamp: CrDS[F, K, V] = transform(sort.descend.timestamp).updateCfg(_.withSorted)

  def repartition(num: Int): CrDS[F, K, V] = transform(_.repartition(num))

  def persist(level: StorageLevel): CrDS[F, K, V] = transform(_.persist(level))
  def unpersist: CrDS[F, K, V]                    = transform(_.unpersist())

  def filter(f: OptionalKV[K, V] => Boolean): CrDS[F, K, V] = transform(_.filter(f))

  def union(other: Dataset[OptionalKV[K, V]]): CrDS[F, K, V] = transform(_.union(other))
  def union(other: CrDS[F, K, V]): CrDS[F, K, V]             = union(other.dataset)

  def distinct: CrDS[F, K, V]  = transform(_.distinct())
  def normalize: CrDS[F, K, V] = transform(ate.normalize(_).dataset)

  // maps
  def bimap[K2, V2](k: K => K2, v: V => V2)(
    other: KafkaTopic[F, K2, V2])(implicit k2: TypedEncoder[K2], v2: TypedEncoder[V2]): CrDS[F, K2, V2] = {
    val ate: AvroTypedEncoder[OptionalKV[K2, V2]] = OptionalKV.ate(other.topicDef)
    new CrDS[F, K2, V2](other, dataset.map(_.bimap(k, v))(ate.sparkEncoder), ate, cfg).normalize
  }

  def map[K2, V2](f: OptionalKV[K, V] => OptionalKV[K2, V2])(
    other: KafkaTopic[F, K2, V2])(implicit k2: TypedEncoder[K2], v2: TypedEncoder[V2]): CrDS[F, K2, V2] = {
    val ate: AvroTypedEncoder[OptionalKV[K2, V2]] = OptionalKV.ate(other.topicDef)
    new CrDS[F, K2, V2](other, dataset.map(f)(ate.sparkEncoder), ate, cfg).normalize

  }

  def flatMap[K2, V2](f: OptionalKV[K, V] => TraversableOnce[OptionalKV[K2, V2]])(
    other: KafkaTopic[F, K2, V2])(implicit k2: TypedEncoder[K2], v2: TypedEncoder[V2]): CrDS[F, K2, V2] = {
    val ate: AvroTypedEncoder[OptionalKV[K2, V2]] = OptionalKV.ate(other.topicDef)
    new CrDS[F, K2, V2](other, dataset.flatMap(f)(ate.sparkEncoder), ate, cfg).normalize
  }

  def stats: Statistics[F] = {
    val enc = TypedExpressionEncoder[CRMetaInfo]
    new Statistics[F](dataset.map(CRMetaInfo(_))(enc), params.timeRange.zoneId)
  }

  def crRdd: CrRdd[F, K, V] = new CrRdd[F, K, V](topic, dataset.rdd, cfg, dataset.sparkSession)
  def prRdd: PrRdd[F, K, V] = new PrRdd[F, K, V](topic, dataset.rdd.map(_.toNJProducerRecord), cfg)

  def save: DatasetAvroFileHoarder[F, OptionalKV[K, V]] =
    new DatasetAvroFileHoarder[F, OptionalKV[K, V]](dataset, ate.avroCodec.avroEncoder)

  def count(implicit F: Sync[F]): F[Long] = F.delay(dataset.count())
}
