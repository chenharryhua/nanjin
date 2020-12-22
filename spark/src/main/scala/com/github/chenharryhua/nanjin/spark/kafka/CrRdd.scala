package com.github.chenharryhua.nanjin.spark.kafka

import cats.Order
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

import scala.reflect.ClassTag

final class CrRdd[F[_], K, V] private[kafka] (
  val topic: KafkaTopic[F, K, V],
  val rdd: RDD[OptionalKV[K, V]],
  val cfg: SKConfig)(implicit val sparkSession: SparkSession)
    extends Serializable {

  protected val codec: AvroCodec[OptionalKV[K, V]] =
    OptionalKV.avroCodec(topic.codec.keySerde.avroCodec, topic.codec.valSerde.avroCodec)

  def params: SKParams = cfg.evalConfig

  def withParamUpdate(f: SKConfig => SKConfig): CrRdd[F, K, V] =
    new CrRdd[F, K, V](topic, rdd, f(cfg))

  def partitionOf(num: Int): CrRdd[F, K, V] =
    new CrRdd[F, K, V](topic, rdd.filter(_.partition === num), cfg)

  // inclusive
  def offsetRange(start: Long, end: Long): CrRdd[F, K, V] =
    new CrRdd[F, K, V](topic, rdd.filter(o => o.offset >= start && o.offset <= end), cfg)

  def offsetAscending: CrRdd[F, K, V]  = sortBy(_.offset, ascending = true)
  def offsetDescending: CrRdd[F, K, V] = sortBy(_.offset, ascending = false)

  def timeRange(dr: NJDateTimeRange): CrRdd[F, K, V] =
    new CrRdd[F, K, V](topic, rdd.filter(m => dr.isInBetween(m.timestamp)), cfg.withTimeRange(dr))

  def timeRange: CrRdd[F, K, V] = timeRange(params.timeRange)

  def timeRange(start: String, end: String): CrRdd[F, K, V] =
    timeRange(params.timeRange.withTimeRange(start, end))

  def sortBy[A: Order: ClassTag](f: OptionalKV[K, V] => A, ascending: Boolean): CrRdd[F, K, V] =
    new CrRdd[F, K, V](topic, rdd.sortBy(f, ascending), cfg)

  def tsAscending: CrRdd[F, K, V]  = sortBy(identity, ascending = true)
  def tsDescending: CrRdd[F, K, V] = sortBy(identity, ascending = false)

  def first(implicit F: Sync[F]): F[Option[OptionalKV[K, V]]] = F.delay(rdd.cminOption)
  def last(implicit F: Sync[F]): F[Option[OptionalKV[K, V]]]  = F.delay(rdd.cmaxOption)

  //transformation
  def normalize: CrRdd[F, K, V] =
    new CrRdd[F, K, V](topic, rdd.map(codec.idConversion), cfg)

  def persist: CrRdd[F, K, V]   = new CrRdd[F, K, V](topic, rdd.persist(), cfg)
  def unpersist: CrRdd[F, K, V] = new CrRdd[F, K, V](topic, rdd.unpersist(), cfg)

  def repartition(num: Int): CrRdd[F, K, V] =
    new CrRdd[F, K, V](topic, rdd.repartition(num), cfg)

  def bimap[K2, V2](k: K => K2, v: V => V2)(other: KafkaTopic[F, K2, V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](other, rdd.map(_.bimap(k, v)), cfg).normalize

  def map[K2, V2](f: OptionalKV[K, V] => OptionalKV[K2, V2])(
    other: KafkaTopic[F, K2, V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](other, rdd.map(f), cfg).normalize

  def flatMap[K2, V2](f: OptionalKV[K, V] => TraversableOnce[OptionalKV[K2, V2]])(
    other: KafkaTopic[F, K2, V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](other, rdd.flatMap(f), cfg).normalize

  def filter(f: OptionalKV[K, V] => Boolean): CrRdd[F, K, V] =
    new CrRdd[F, K, V](topic, rdd.filter(f), cfg)

  def union(other: RDD[OptionalKV[K, V]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](topic, rdd.union(other), cfg)

  def union(other: CrRdd[F, K, V]): CrRdd[F, K, V] = union(other.rdd)

  def dismissNulls: CrRdd[F, K, V] =
    new CrRdd[F, K, V](topic, rdd.dismissNulls, cfg)

  def replicate(num: Int): CrRdd[F, K, V] = {
    val rep = (1 until num).foldLeft(rdd) { case (r, _) => r.union(rdd) }
    new CrRdd[F, K, V](topic, rep, cfg)
  }

  def distinct: CrRdd[F, K, V] = new CrRdd[F, K, V](topic, rdd.distinct(), cfg)

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
    new Statistics[F](TypedDataset.create(rdd.map(CRMetaInfo(_))).dataset, cfg)

}
