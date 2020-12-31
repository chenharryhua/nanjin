package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.persist.RddAvroFileHoarder
import frameless.cats.implicits.rddOps
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

final class CrRdd[F[_], K, V] private[kafka] (
  val rdd: RDD[OptionalKV[K, V]],
  topic: KafkaTopic[F, K, V],
  cfg: SKConfig,
  ss: SparkSession)
    extends Serializable {

  protected val codec: AvroCodec[OptionalKV[K, V]] = OptionalKV.avroCodec(topic.topicDef)

  def params: SKParams = cfg.evalConfig

  // transforms
  def transform(f: RDD[OptionalKV[K, V]] => RDD[OptionalKV[K, V]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](f(rdd), topic, cfg, ss)

  def partitionOf(num: Int): CrRdd[F, K, V] = transform(_.filter(_.partition === num))

  def offsetRange(start: Long, end: Long): CrRdd[F, K, V] = transform(range.cr.offset(start, end))
  def timeRange(dr: NJDateTimeRange): CrRdd[F, K, V]      = transform(range.cr.timestamp(dr))
  def timeRange: CrRdd[F, K, V]                           = timeRange(params.timeRange)

  def ascendTimestamp: CrRdd[F, K, V]  = transform(sort.ascend.cr.timestamp)
  def descendTimestamp: CrRdd[F, K, V] = transform(sort.descend.cr.timestamp)
  def ascendOffset: CrRdd[F, K, V]     = transform(sort.ascend.cr.offset)
  def descendOffset: CrRdd[F, K, V]    = transform(sort.descend.cr.offset)

  def union(other: CrRdd[F, K, V]): CrRdd[F, K, V] = transform(_.union(other.rdd))
  def repartition(num: Int): CrRdd[F, K, V]        = transform(_.repartition(num))

  def normalize: CrRdd[F, K, V] = transform(_.map(codec.idConversion))

  def dismissNulls: CrRdd[F, K, V] = transform(_.dismissNulls)

  // maps
  def bimap[K2, V2](k: K => K2, v: V => V2)(other: KafkaTopic[F, K2, V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](rdd.map(_.bimap(k, v)), other, cfg, ss).normalize

  def map[K2, V2](f: OptionalKV[K, V] => OptionalKV[K2, V2])(other: KafkaTopic[F, K2, V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](rdd.map(f), other, cfg, ss).normalize

  def flatMap[K2, V2](f: OptionalKV[K, V] => TraversableOnce[OptionalKV[K2, V2]])(
    other: KafkaTopic[F, K2, V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](rdd.flatMap(f), other, cfg, ss).normalize

  // dataset
  def crDS(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate = OptionalKV.ate(topic.topicDef)
    new CrDS[F, K, V](ss.createDataset(rdd)(ate.sparkEncoder), topic, cfg, tek, tev)
  }

  def values: RDD[CompulsoryV[K, V]]     = rdd.flatMap(_.toCompulsoryV)
  def keys: RDD[CompulsoryK[K, V]]       = rdd.flatMap(_.toCompulsoryK)
  def keyValues: RDD[CompulsoryKV[K, V]] = rdd.flatMap(_.toCompulsoryKV)

  // upload
  def prRdd: PrRdd[F, K, V] = new PrRdd[F, K, V](rdd.map(_.toNJProducerRecord), topic, cfg)

  def upload(implicit ce: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): F[Unit] =
    prRdd.noMeta.upload(topic).map(_ => print(".")).compile.drain

  // save
  def save: RddAvroFileHoarder[F, OptionalKV[K, V]] =
    new RddAvroFileHoarder[F, OptionalKV[K, V]](rdd, codec.avroEncoder)

  def count(implicit F: Sync[F]): F[Long] = F.delay(rdd.count())

  def stats: Statistics[F] =
    new Statistics[F](
      TypedDataset.create(rdd.map(CRMetaInfo(_)))(TypedEncoder[CRMetaInfo], ss).dataset,
      params.timeRange.zoneId)
}
