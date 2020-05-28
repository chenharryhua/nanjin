package com.github.chenharryhua.nanjin.spark.kafka

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.github.chenharryhua.nanjin.pipes.hadoop
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Queue

final class FsmRdd[F[_], K, V](
  rdd: RDD[NJConsumerRecord[K, V]],
  topic: KafkaTopic[F, K, V],
  cfg: SKConfig)(implicit sparkSession: SparkSession)
    extends SparKafkaUpdateParams[FsmRdd[F, K, V]] {

  import topic.topicDef.{avroKeyEncoder, avroValEncoder, schemaForKey, schemaForVal}

  override def params: SKParams = SKConfigF.evalConfig(cfg)

  override def withParamUpdate(f: SKConfig => SKConfig): FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd, topic, f(cfg))

  def partition(num: Int): FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd.filter(_.partition === num), topic, cfg)

  def save(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] =
    Blocker[F].use { blocker =>
      val path = sk.replayPath(topic.topicName)
      hadoop.delete(path, sparkSession.sparkContext.hadoopConfiguration, blocker) >>
        F.delay(rdd.saveAsObjectFile(path))
    }

  def count: Long = rdd.count()

  def stats: Statistics[F] =
    new Statistics(TypedDataset.create(rdd.map(CRMetaInfo(_))).dataset, cfg)

  def crDataset(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmConsumerRecords[F, K, V] = {
    val tds       = TypedDataset.create(rdd)
    val inBetween = tds.makeUDF[Long, Boolean](params.timeRange.isInBetween)
    new FsmConsumerRecords(tds.filter(inBetween(tds('timestamp))).dataset, topic, cfg)
  }

  // sorted RDD
  def sorted: RDD[NJConsumerRecord[K, V]] =
    rdd.filter(m => params.timeRange.isInBetween(m.timestamp)).sortBy(identity)

  def crStream(implicit F: Sync[F]): Stream[F, NJConsumerRecord[K, V]] =
    sorted.stream[F]

  def crSource(implicit F: ConcurrentEffect[F]): Source[NJConsumerRecord[K, V], NotUsed] =
    sorted.source[F]

  def malOrdered(implicit F: Sync[F]): Stream[F, NJConsumerRecord[K, V]] =
    crStream.sliding(2).mapFilter {
      case Queue(a, b) => if (a.timestamp <= b.timestamp) None else Some(a)
    }

  def saveJackson(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] =
    crStream.through(fileSink.jackson(sk.jacksonPath(topic.topicName))).compile.drain

  def saveAvro(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] =
    crStream
      .through(fileSink.avro(sk.avroPath(topic.topicName), topic.topicDef.crAvroSchema))
      .compile
      .drain

  def pipeTo(otherTopic: KafkaTopic[F, K, V])(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): F[Unit] =
    crStream
      .map(_.toNJProducerRecord.noMeta)
      .through(sk.uploader(otherTopic, params.uploadRate))
      .map(_ => print("."))
      .compile
      .drain

  def replay(implicit ce: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): F[Unit] =
    pipeTo(topic)

}
