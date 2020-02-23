package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync, Timer}
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.github.chenharryhua.nanjin.spark.hadoop
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import cats.implicits._

final class FsmRddKafka[F[_], K, V](
  rdd: RDD[NJConsumerRecord[K, V]],
  kit: KafkaTopicKit[K, V],
  cfg: SKConfig)(implicit sparkSession: SparkSession)
    extends SparKafkaUpdateParams[FsmRddKafka[F, K, V]] {
  override def params: SKParams = SKConfigF.evalConfig(cfg)

  override def withParamUpdate(f: SKConfig => SKConfig): FsmRddKafka[F, K, V] =
    new FsmRddKafka[F, K, V](rdd, kit, f(cfg))

  def save(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] =
    Blocker[F].use { blocker =>
      val path = params.rddPathBuilder(kit.topicName)
      hadoop.delete(path, sparkSession.sparkContext.hadoopConfiguration, blocker) >>
        F.delay(rdd.saveAsObjectFile(path))
    }

  def count: Long = rdd.count()

  def crDataset(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords(TypedDataset.create(rdd).dataset, kit, cfg)

  def sorted: RDD[NJConsumerRecord[K, V]] =
    rdd.sortBy[NJConsumerRecord[K, V]](identity).repartition(params.repartition.value)

  def crStream(implicit F: Sync[F]): Stream[F, NJConsumerRecord[K, V]] =
    Stream.fromIterator[F](sorted.toLocalIterator)

  def pipeTo(otherTopic: KafkaTopicKit[K, V])(
    implicit
    concurrentEffect: ConcurrentEffect[F],
    timer: Timer[F],
    contextShift: ContextShift[F]): F[Unit] =
    crStream
      .map(_.toNJProducerRecord.noMeta)
      .chunkN(params.uploadRate.batchSize)
      .metered(params.uploadRate.duration)
      .through(otherTopic.upload)
      .map(_ => print("."))
      .compile
      .drain

  def stats: Statistics[F] =
    new Statistics(TypedDataset.create(rdd.map(CRMetaInfo(_))).dataset, cfg)
}
