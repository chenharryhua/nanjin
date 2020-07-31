package com.github.chenharryhua.nanjin.spark.kafka

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.Order
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.{
  CompulsoryK,
  CompulsoryKV,
  CompulsoryV,
  OptionalKV
}
import com.github.chenharryhua.nanjin.spark.{RddExt, RddToDataFrame}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.cats.implicits.rddOps
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.ClassTag

final class CrRdd[F[_], K, V](
  val rdd: RDD[OptionalKV[K, V]],
  val topicName: TopicName,
  val cfg: SKConfig)(implicit
  val sparkSession: SparkSession,
  val keyAvroEncoder: AvroEncoder[K],
  val valAvroEncoder: AvroEncoder[V])
    extends SparKafkaUpdateParams[CrRdd[F, K, V]] with CrRddSaveModule[F, K, V]
    with CrRddInvModule[F, K, V] {

  override def params: SKParams = cfg.evalConfig

  override def withParamUpdate(f: SKConfig => SKConfig): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd, topicName, f(cfg))

  def withTopicName(tn: String): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd, TopicName.unsafeFrom(tn), cfg)

  //transformation

  def kafkaPartition(num: Int): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.filter(_.partition === num), topicName, cfg)

  def sortBy[A: Order: ClassTag](f: OptionalKV[K, V] => A, ascending: Boolean) =
    new CrRdd[F, K, V](rdd.sortBy(f, ascending), topicName, cfg)

  def ascending: CrRdd[F, K, V]  = sortBy(identity, ascending = true)
  def descending: CrRdd[F, K, V] = sortBy(identity, ascending = false)

  def repartition(num: Int): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.repartition(num), topicName, cfg)

  def bimap[K2: AvroEncoder, V2: AvroEncoder](k: K => K2, v: V => V2): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](rdd.map(_.bimap(k, v)), topicName, cfg)

  def flatMap[K2: AvroEncoder, V2: AvroEncoder](
    f: OptionalKV[K, V] => TraversableOnce[OptionalKV[K2, V2]]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](rdd.flatMap(f), topicName, cfg)

  def union(other: RDD[OptionalKV[K, V]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.union(other), topicName, cfg)

  def union(other: CrRdd[F, K, V]): CrRdd[F, K, V] = union(other.rdd)

  def dismissNulls: CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.dismissNulls, topicName, cfg)

  def inRange(dr: NJDateTimeRange): CrRdd[F, K, V] =
    new CrRdd[F, K, V](
      rdd.filter(m => dr.isInBetween(m.timestamp)),
      topicName,
      cfg.withTimeRange(dr))

  def inRange: CrRdd[F, K, V] = inRange(params.timeRange)

  def inRange(start: String, end: String): CrRdd[F, K, V] =
    inRange(params.timeRange.withTimeRange(start, end))

  // out of FsmRdd

  def typedDataset(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): TypedDataset[OptionalKV[K, V]] =
    TypedDataset.create(rdd)

  def toDF: DataFrame =
    new RddToDataFrame[OptionalKV[K, V]](rdd, AvroEncoder[OptionalKV[K, V]], sparkSession).toDF

  def crDataset(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): CrDataset[F, K, V] =
    new CrDataset(typedDataset.dataset, cfg)(keyEncoder, keyAvroEncoder, valEncoder, valAvroEncoder)

  def stream(implicit F: Sync[F]): Stream[F, OptionalKV[K, V]] =
    rdd.stream[F]

  def source(implicit F: ConcurrentEffect[F]): Source[OptionalKV[K, V], NotUsed] =
    rdd.source[F]

  // rdd
  def values: RDD[CompulsoryV[K, V]]     = rdd.flatMap(_.toCompulsoryV)
  def keys: RDD[CompulsoryK[K, V]]       = rdd.flatMap(_.toCompulsoryK)
  def keyValues: RDD[CompulsoryKV[K, V]] = rdd.flatMap(_.toCompulsoryKV)

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
}
