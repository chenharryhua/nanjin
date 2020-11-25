package com.github.chenharryhua.nanjin.spark.kafka

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.Order
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.persist.RddFileHoarder
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, RddExt}
import frameless.cats.implicits.rddOps
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class CrRdd[F[_], K, V](val rdd: RDD[OptionalKV[K, V]], val cfg: SKConfig)(implicit
  val keyCodec: AvroCodec[K],
  val valCodec: AvroCodec[V],
  val sparkSession: SparkSession)
    extends SparKafkaUpdateParams[CrRdd[F, K, V]] with InvModule[F, K, V] {

  private val codec: AvroCodec[OptionalKV[K, V]] = shapeless.cachedImplicit

  override def params: SKParams = cfg.evalConfig

  override def withParamUpdate(f: SKConfig => SKConfig): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd, f(cfg))

  //transformation

  def partitionOf(num: Int): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.filter(_.partition === num), cfg)

  def sortBy[A: Order: ClassTag](f: OptionalKV[K, V] => A, ascending: Boolean) =
    new CrRdd[F, K, V](rdd.sortBy(f, ascending), cfg)

  def ascending: CrRdd[F, K, V]  = sortBy(identity, ascending = true)
  def descending: CrRdd[F, K, V] = sortBy(identity, ascending = false)

  def repartition(num: Int): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.repartition(num), cfg)

  def bimap[K2, V2](k: K => K2, v: V => V2)(implicit
    ck2: AvroCodec[K2],
    cv2: AvroCodec[V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](rdd.map(_.bimap(k, v)), cfg)

  def mapValues[V2](v: V => V2)(implicit cv2: AvroCodec[V2]): CrRdd[F, K, V2] =
    bimap[K, V2](identity, v)

  def mapKeys[K2](k: K => K2)(implicit ck2: AvroCodec[K2]): CrRdd[F, K2, V] =
    bimap(k, identity)

  def flatMap[K2, V2](f: OptionalKV[K, V] => TraversableOnce[OptionalKV[K2, V2]])(implicit
    ck2: AvroCodec[K2],
    cv2: AvroCodec[V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](rdd.flatMap(f), cfg)

  def filter(f: OptionalKV[K, V] => Boolean): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.filter(f), cfg)

  def take(num: Int): CrRdd[F, K, V] =
    new CrRdd[F, K, V](sparkSession.sparkContext.parallelize(rdd.take(num)), cfg)

  def union(other: RDD[OptionalKV[K, V]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.union(other), cfg)

  def union(other: CrRdd[F, K, V]): CrRdd[F, K, V] = union(other.rdd)

  def dismissNulls: CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.dismissNulls, cfg)

  def inRange(dr: NJDateTimeRange): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.filter(m => dr.isInBetween(m.timestamp)), cfg.withTimeRange(dr))

  def inRange: CrRdd[F, K, V] = inRange(params.timeRange)

  def inRange(start: String, end: String): CrRdd[F, K, V] =
    inRange(params.timeRange.withTimeRange(start, end))

  // dataset
  def typedDataset(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): TypedDataset[OptionalKV[K, V]] = {
    val ate: AvroTypedEncoder[OptionalKV[K, V]] = AvroTypedEncoder(codec)
    ate.normalize(rdd)
  }

  def valueSet(implicit valEncoder: TypedEncoder[V]): TypedDataset[V] = {
    val ate: AvroTypedEncoder[V] = AvroTypedEncoder(valCodec)
    ate.normalize(rdd.flatMap(_.value)(valEncoder.classTag))
  }

  def keySet(implicit keyEncoder: TypedEncoder[K]): TypedDataset[K] = {
    val ate: AvroTypedEncoder[K] = AvroTypedEncoder(keyCodec)
    ate.normalize(rdd.flatMap(_.key)(keyEncoder.classTag))
  }

  def values: RDD[CompulsoryV[K, V]]     = rdd.flatMap(_.toCompulsoryV)
  def keys: RDD[CompulsoryK[K, V]]       = rdd.flatMap(_.toCompulsoryK)
  def keyValues: RDD[CompulsoryKV[K, V]] = rdd.flatMap(_.toCompulsoryKV)

  // streams
  def stream(implicit F: Sync[F]): Stream[F, OptionalKV[K, V]] =
    rdd.stream[F]

  def source(implicit F: ConcurrentEffect[F]): Source[OptionalKV[K, V], NotUsed] =
    rdd.source[F]

  def toPrRdd: PrRdd[F, K, V] =
    new PrRdd[F, K, V](rdd.map(_.toNJProducerRecord), cfg)

  def replicate(num: Int): CrRdd[F, K, V] = {
    val rep = (0 until num).foldLeft(rdd) { case (r, _) => r.union(rdd) }
    new CrRdd[F, K, V](rep, cfg)
  }

  // pipe
  def pipeTo(otherTopic: KafkaTopic[F, K, V])(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): F[Unit] =
    stream
      .map(_.toNJProducerRecord.noMeta)
      .through(sk.uploader(otherTopic, params.uploadRate))
      .map(_ => print("."))
      .compile
      .drain

  def save: RddFileHoarder[F, OptionalKV[K, V]] =
    new RddFileHoarder[F, OptionalKV[K, V]](rdd, codec)
}
