package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.persist.RddAvroFileHoarder
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, RddExt}
import frameless.cats.implicits.rddOps
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

final class CrRdd[F[_], K, V] private[kafka] (
  val topic: KafkaTopic[F, K, V],
  val rdd: RDD[OptionalKV[K, V]],
  val cfg: SKConfig,
  val sparkSession: SparkSession)
    extends Serializable {

  protected val codec: AvroCodec[OptionalKV[K, V]] =
    OptionalKV.avroCodec(topic.codec.keySerde.avroCodec, topic.codec.valSerde.avroCodec)

  def params: SKParams = cfg.evalConfig

  def withParamUpdate(f: SKConfig => SKConfig): CrRdd[F, K, V] =
    new CrRdd[F, K, V](topic, rdd, f(cfg), sparkSession)

  // transforms
  def transform(f: RDD[OptionalKV[K, V]] => RDD[OptionalKV[K, V]]) =
    new CrRdd[F, K, V](topic, f(rdd), cfg, sparkSession)

  def partitionOf(num: Int): CrRdd[F, K, V] = transform(_.filter(_.partition === num))

  def offsetRange(start: Long, end: Long): CrRdd[F, K, V] =
    transform(_.filter(o => o.offset >= start && o.offset <= end))

  def timeRange(dr: NJDateTimeRange): CrRdd[F, K, V] =
    transform(_.filter(m => dr.isInBetween(m.timestamp)))

  def timeRange(start: String, end: String): CrRdd[F, K, V] =
    timeRange(params.timeRange.withTimeRange(start, end))

  def timeRange: CrRdd[F, K, V] = timeRange(params.timeRange)

  def ascendTimestamp: CrRdd[F, K, V]  = transform(sort.ascending.cr.timestamp)
  def descendTimestamp: CrRdd[F, K, V] = transform(sort.descending.cr.timestamp)
  def ascendOffset: CrRdd[F, K, V]     = transform(sort.ascending.cr.offset)
  def descendOffset: CrRdd[F, K, V]    = transform(sort.descending.cr.offset)

  def first(implicit F: Sync[F]): F[Option[OptionalKV[K, V]]] = F.delay(rdd.cminOption)
  def last(implicit F: Sync[F]): F[Option[OptionalKV[K, V]]]  = F.delay(rdd.cmaxOption)

  def persist: CrRdd[F, K, V]   = transform(_.persist())
  def unpersist: CrRdd[F, K, V] = transform(_.unpersist())

  def repartition(num: Int): CrRdd[F, K, V]                  = transform(_.repartition(num))
  def filter(f: OptionalKV[K, V] => Boolean): CrRdd[F, K, V] = transform(_.filter(f))

  def union(other: RDD[OptionalKV[K, V]]): CrRdd[F, K, V] = transform(_.union(other))
  def union(other: CrRdd[F, K, V]): CrRdd[F, K, V]        = union(other.rdd)

  def distinct: CrRdd[F, K, V] = transform(_.distinct())

  def normalize: CrRdd[F, K, V] = transform(_.map(codec.idConversion))

  // maps

  def bimap[K2, V2](k: K => K2, v: V => V2)(other: KafkaTopic[F, K2, V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](other, rdd.map(_.bimap(k, v)), cfg, sparkSession).normalize

  def map[K2, V2](f: OptionalKV[K, V] => OptionalKV[K2, V2])(
    other: KafkaTopic[F, K2, V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](other, rdd.map(f), cfg, sparkSession).normalize

  def flatMap[K2, V2](f: OptionalKV[K, V] => TraversableOnce[OptionalKV[K2, V2]])(
    other: KafkaTopic[F, K2, V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](other, rdd.flatMap(f), cfg, sparkSession).normalize

  def dismissNulls: CrRdd[F, K, V] =
    new CrRdd[F, K, V](topic, rdd.dismissNulls, cfg, sparkSession)

  def replicate(num: Int): CrRdd[F, K, V] = {
    val rep = (1 until num).foldLeft(rdd) { case (r, _) => r.union(rdd) }
    new CrRdd[F, K, V](topic, rep, cfg, sparkSession)
  }

  // dataset
  def crDS(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate: AvroTypedEncoder[OptionalKV[K, V]] =
      OptionalKV.ate(topic.codec.keySerde.avroCodec, topic.codec.valSerde.avroCodec)

    val tds = TypedDataset.create(rdd)(ate.typedEncoder, sparkSession)
    new CrDS[F, K, V](topic, tds.dataset, ate, cfg)
  }

  def values: RDD[CompulsoryV[K, V]]     = rdd.flatMap(_.toCompulsoryV)
  def keys: RDD[CompulsoryK[K, V]]       = rdd.flatMap(_.toCompulsoryK)
  def keyValues: RDD[CompulsoryKV[K, V]] = rdd.flatMap(_.toCompulsoryKV)

  // upload
  def prRdd: PrRdd[F, K, V] = new PrRdd[F, K, V](topic, rdd.map(_.toNJProducerRecord), cfg)

  def upload(implicit ce: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): F[Unit] =
    prRdd.noMeta.upload(topic).map(_ => print(".")).compile.drain

  // save
  def save: RddAvroFileHoarder[F, OptionalKV[K, V]] =
    new RddAvroFileHoarder[F, OptionalKV[K, V]](rdd, codec.avroEncoder)

  def count(implicit F: Sync[F]): F[Long] = F.delay(rdd.count())

  def stats: Statistics[F] =
    new Statistics[F](
      TypedDataset.create(rdd.map(CRMetaInfo(_)))(TypedEncoder[CRMetaInfo], sparkSession).dataset,
      cfg)
}
