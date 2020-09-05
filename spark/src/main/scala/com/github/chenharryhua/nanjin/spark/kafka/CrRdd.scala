package com.github.chenharryhua.nanjin.spark.kafka

import java.time.LocalDate

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.Order
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.{localdateInstances, NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.persist.{RddFileHoarder, RddPartitionHoarder}
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, RddExt}
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import frameless.cats.implicits.rddOps
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class CrRdd[F[_], K, V](
  val rdd: RDD[OptionalKV[K, V]],
  val codec: KafkaAvroCodec[K, V],
  val cfg: SKConfig)(implicit val sparkSession: SparkSession)
    extends SparKafkaUpdateParams[CrRdd[F, K, V]] with CrRddInvModule[F, K, V] {

  override def params: SKParams = cfg.evalConfig

  override def withParamUpdate(f: SKConfig => SKConfig): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd, codec, f(cfg))

  //transformation

  def kafkaPartition(num: Int): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.filter(_.partition === num), codec, cfg)

  def sortBy[A: Order: ClassTag](f: OptionalKV[K, V] => A, ascending: Boolean) =
    new CrRdd[F, K, V](rdd.sortBy(f, ascending), codec, cfg)

  def ascending: CrRdd[F, K, V]  = sortBy(identity, ascending = true)
  def descending: CrRdd[F, K, V] = sortBy(identity, ascending = false)

  def repartition(num: Int): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.repartition(num), codec, cfg)

  def bimap[K2: AvroEncoder: AvroDecoder: SchemaFor, V2: AvroEncoder: AvroDecoder: SchemaFor](
    k: K => K2,
    v: V => V2): CrRdd[F, K2, V2] = {
    val codec = new KafkaAvroCodec[K2, V2](AvroCodec[K2], AvroCodec[V2])
    new CrRdd[F, K2, V2](rdd.map(_.bimap(k, v)), codec, cfg)
  }

  def flatMap[K2: AvroEncoder: AvroDecoder: SchemaFor, V2: AvroEncoder: AvroDecoder: SchemaFor](
    f: OptionalKV[K, V] => TraversableOnce[OptionalKV[K2, V2]]): CrRdd[F, K2, V2] = {
    val codec = new KafkaAvroCodec[K2, V2](AvroCodec[K2], AvroCodec[V2])
    new CrRdd[F, K2, V2](rdd.flatMap(f), codec, cfg)
  }

  def union(other: RDD[OptionalKV[K, V]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.union(other), codec, cfg)

  def union(other: CrRdd[F, K, V]): CrRdd[F, K, V] = union(other.rdd)

  def dismissNulls: CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.dismissNulls, codec, cfg)

  def inRange(dr: NJDateTimeRange): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd.filter(m => dr.isInBetween(m.timestamp)), codec, cfg.withTimeRange(dr))

  def inRange: CrRdd[F, K, V] = inRange(params.timeRange)

  def inRange(start: String, end: String): CrRdd[F, K, V] =
    inRange(params.timeRange.withTimeRange(start, end))

  // out of FsmRdd

  def typedDataset(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): TypedDataset[OptionalKV[K, V]] = {
    val ate = AvroTypedEncoder[OptionalKV[K, V]](codec.optionalKVCodec)
    ate.normalize(rdd)
  }

  def crDataset(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): CrDataset[F, K, V] = {
    val c = new KafkaAvroTypedEncoder[K, V](keyEncoder, valEncoder, codec)
    new CrDataset(typedDataset.dataset, c, cfg)
  }

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

  def save: RddFileHoarder[F, OptionalKV[K, V]] = {
    implicit val c: AvroCodec[OptionalKV[K, V]] = codec.optionalKVCodec
    new RddFileHoarder[F, OptionalKV[K, V]](rdd)
  }

  private def bucketing(kv: OptionalKV[K, V]): Option[LocalDate] =
    Some(NJTimestamp(kv.timestamp).dayResolution(params.timeRange.zoneId))

  def partition: RddPartitionHoarder[F, OptionalKV[K, V], LocalDate] = {
    implicit val c: AvroCodec[OptionalKV[K, V]] = codec.optionalKVCodec
    new RddPartitionHoarder[F, OptionalKV[K, V], LocalDate](
      rdd,
      bucketing,
      params.datePartitionPathBuilder(params.topicName, _, _))
  }
}
