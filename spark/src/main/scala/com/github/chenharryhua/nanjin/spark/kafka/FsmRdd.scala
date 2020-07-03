package com.github.chenharryhua.nanjin.spark.kafka

import java.io.ByteArrayOutputStream

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.Show
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.{
  CompulsoryK,
  CompulsoryKV,
  CompulsoryV,
  OptionalKV
}
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import com.sksamuel.avro4s.{AvroOutputStream, Encoder => AvroEncoder}
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import io.circe.generic.auto._
import io.circe.{Encoder => JsonEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Queue

final class FsmRdd[F[_], K, V](val rdd: RDD[OptionalKV[K, V]], topicName: TopicName, cfg: SKConfig)(
  implicit
  sparkSession: SparkSession,
  avroKeyEncoder: AvroEncoder[K],
  avroValEncoder: AvroEncoder[V])
    extends SparKafkaUpdateParams[FsmRdd[F, K, V]] {

  override def params: SKParams = cfg.evalConfig

  override def withParamUpdate(f: SKConfig => SKConfig): FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd, topicName, f(cfg))

  //transformation

  def kafkaPartition(num: Int): FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd.filter(_.partition === num), topicName, cfg)

  def ascending: FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd.sortBy(identity, ascending = true), topicName, cfg)

  def descending: FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd.sortBy(identity, ascending = false), topicName, cfg)

  def repartition(num: Int): FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd.repartition(num), topicName, cfg)

  def bimap[K2: AvroEncoder, V2: AvroEncoder](k: K => K2, v: V => V2): FsmRdd[F, K2, V2] =
    new FsmRdd[F, K2, V2](rdd.map(_.bimap(k, v)), topicName, cfg)

  def flatMap[K2: AvroEncoder, V2: AvroEncoder](
    f: OptionalKV[K, V] => TraversableOnce[OptionalKV[K2, V2]]): FsmRdd[F, K2, V2] =
    new FsmRdd[F, K2, V2](rdd.flatMap(f), topicName, cfg)

  def dismissNulls: FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd.dismissNulls, topicName, cfg)

  // out of FsmRdd

  def typedDataset(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): TypedDataset[OptionalKV[K, V]] =
    TypedDataset.create(rdd)

  def crDataset(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords(typedDataset.dataset, cfg)

  def stream(implicit F: Sync[F]): Stream[F, OptionalKV[K, V]] =
    rdd.stream[F]

  def source(implicit F: ConcurrentEffect[F]): Source[OptionalKV[K, V], NotUsed] =
    rdd.source[F]

  def malOrdered(implicit F: Sync[F]): Stream[F, OptionalKV[K, V]] =
    stream.sliding(2).mapFilter {
      case Queue(a, b) => if (a.timestamp <= b.timestamp) None else Some(a)
    }

  // rdd
  def values: RDD[CompulsoryV[K, V]]     = rdd.flatMap(_.compulsoryV)
  def keys: RDD[CompulsoryK[K, V]]       = rdd.flatMap(_.compulsoryK)
  def keyValues: RDD[CompulsoryKV[K, V]] = rdd.flatMap(_.compulsoryKV)

  // investigation
  def count: Long = rdd.count()

  def stats: Statistics[F] =
    new Statistics(TypedDataset.create(rdd.map(CRMetaInfo(_))).dataset, cfg)

  def missingData: TypedDataset[CRMetaInfo] =
    inv.missingData(TypedDataset.create(values.map(CRMetaInfo(_))))

  def diff(other: RDD[OptionalKV[K, V]]): TypedDataset[(KafkaMsgDigest, Option[KafkaMsgDigest])] = {
    val mine: RDD[KafkaMsgDigest] =
      rdd.map(m => KafkaMsgDigest(m.partition, m.offset, m.key.hashCode(), m.value.hashCode()))
    val yours: RDD[KafkaMsgDigest] =
      other.map(m => KafkaMsgDigest(m.partition, m.offset, m.key.hashCode(), m.value.hashCode()))
    inv.compareDataset(TypedDataset.create(mine), TypedDataset.create(yours))
  }

  def dupRecords: TypedDataset[(Int, Long, Long)] =
    inv.dupRecords(TypedDataset.create(values.map(CRMetaInfo(_))))

  // dump java object
  def dump(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] =
    Blocker[F].use { blocker =>
      val pathStr = params.replayPath(topicName)
      fileSink(blocker).delete(pathStr) >> F.delay(rdd.saveAsObjectFile(pathStr))
    }

  //save actions
  def saveJson(blocker: Blocker)(implicit
    ce: Sync[F],
    cs: ContextShift[F],
    ek: JsonEncoder[K],
    ev: JsonEncoder[V]): F[Long] = {
    val path = params.pathBuilder(topicName, NJFileFormat.Json)
    val data = rdd.persist()
    stream
      .through(fileSink(blocker).json[OptionalKV[K, V]](path))
      .compile
      .drain
      .as(data.count()) <* ce.delay(data.unpersist())
  }

  def saveText(blocker: Blocker)(implicit
    showK: Show[K],
    showV: Show[V],
    ce: Sync[F],
    cs: ContextShift[F]): F[Long] = {
    val path = params.pathBuilder(topicName, NJFileFormat.Text)
    val data = rdd.persist()
    stream
      .through(fileSink[F](blocker).text[OptionalKV[K, V]](path))
      .compile
      .drain
      .as(data.count()) <*
      ce.delay(data.unpersist())
  }

  private def avroLike(blocker: Blocker, fmt: NJFileFormat)(implicit
    ce: ConcurrentEffect[F],
    cs: ContextShift[F]): F[Long] = {

    val path = params.pathBuilder(topicName, fmt)
    val data = rdd.persist()
    val run: Stream[F, Unit] = fmt match {
      case NJFileFormat.Avro       => data.stream[F].through(fileSink(blocker).avro(path))
      case NJFileFormat.BinaryAvro => data.stream[F].through(fileSink(blocker).binaryAvro(path))
      case NJFileFormat.Jackson    => data.stream[F].through(fileSink(blocker).jackson(path))
      case NJFileFormat.Parquet    => data.stream[F].through(fileSink(blocker).parquet(path))
      case NJFileFormat.JavaObject => data.stream[F].through(fileSink(blocker).javaObject(path))
      case _                       => sys.error("never happen")
    }
    run.compile.drain.as(data.count) <* ce.delay(data.unpersist())
  }

  def saveJackson(
    blocker: Blocker)(implicit ce: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    avroLike(blocker, NJFileFormat.Jackson)

  def saveAvro(blocker: Blocker)(implicit ce: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    avroLike(blocker, NJFileFormat.Avro)

  def saveBinaryAvro(
    blocker: Blocker)(implicit ce: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    avroLike(blocker, NJFileFormat.BinaryAvro)

  def saveParquet(
    blocker: Blocker)(implicit ce: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    avroLike(blocker, NJFileFormat.Parquet)

  def saveJavaObject(
    blocker: Blocker)(implicit ce: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    avroLike(blocker, NJFileFormat.JavaObject)

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
