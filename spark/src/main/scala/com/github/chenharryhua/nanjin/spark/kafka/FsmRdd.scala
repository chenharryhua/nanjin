package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.github.chenharryhua.nanjin.pipes.hadoop
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

final class FsmRdd[F[_], K, V](
  rdd: RDD[NJConsumerRecord[K, V]],
  kit: KafkaTopicKit[K, V],
  cfg: SKConfig)(implicit sparkSession: SparkSession)
    extends SparKafkaUpdateParams[FsmRdd[F, K, V]] {

  def save(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] =
    Blocker[F].use { blocker =>
      val path = sk.replayPath(kit.topicName)
      hadoop.delete(path, sparkSession.sparkContext.hadoopConfiguration, blocker) >>
        F.delay(rdd.saveAsObjectFile(path))
    }

  def saveJackson(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {
    import kit.topicDef.{avroKeyEncoder, avroValEncoder, schemaForKey, schemaForVal}
    rdd
      .stream[F]
      .through(fileSink[F].jackson[NJConsumerRecord[K, V]](sk.jacksonPath(kit.topicName)))
      .compile
      .drain
  }

  def saveAvro(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {
    import kit.topicDef.{avroKeyEncoder, avroValEncoder}
    rdd
      .stream[F]
      .through(
        fileSink[F]
          .avro[NJConsumerRecord[K, V]](sk.jacksonPath(kit.topicName), kit.topicDef.njAvroSchema))
      .compile
      .drain
  }

  def replay(
    implicit
    concurrentEffect: ConcurrentEffect[F],
    timer: Timer[F],
    contextShift: ContextShift[F]): F[Unit] =
    crStream
      .map(_.toNJProducerRecord.noMeta)
      .through(sk.uploader(kit, params.uploadRate))
      .map(_ => print("."))
      .compile
      .drain

  def count: Long = rdd.count()

  override def params: SKParams = SKConfigF.evalConfig(cfg)

  override def withParamUpdate(f: SKConfig => SKConfig): FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd, kit, f(cfg))

  def crDataset(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmConsumerRecords[F, K, V] = {
    val tds       = TypedDataset.create(rdd)
    val inBetween = tds.makeUDF[Long, Boolean](params.timeRange.isInBetween)
    new FsmConsumerRecords(tds.filter(inBetween(tds('timestamp))).dataset, kit, cfg)
  }

  def partition(num: Int): FsmRdd[F, K, V] =
    new FsmRdd[F, K, V](rdd.filter(_.partition === num), kit, cfg)

  def sorted: RDD[NJConsumerRecord[K, V]] =
    rdd
      .filter(m => params.timeRange.isInBetween(m.timestamp))
      .repartition(params.repartition.value)
      .sortBy[NJConsumerRecord[K, V]](identity)

  def crStream(implicit F: Sync[F]): Stream[F, NJConsumerRecord[K, V]] =
    sorted.stream[F]

  def stats: Statistics[F] =
    new Statistics(TypedDataset.create(rdd.map(CRMetaInfo(_))).dataset, cfg)

  def pipeTo(otherTopic: KafkaTopicKit[K, V])(
    implicit
    concurrentEffect: ConcurrentEffect[F],
    timer: Timer[F],
    contextShift: ContextShift[F]): F[Unit] =
    crStream
      .map(_.toNJProducerRecord.noMeta)
      .through(sk.uploader(otherTopic, params.uploadRate))
      .map(_ => print("."))
      .compile
      .drain
}
