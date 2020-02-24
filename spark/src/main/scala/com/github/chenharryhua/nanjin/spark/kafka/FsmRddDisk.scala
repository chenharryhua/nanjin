package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

final class FsmRddDisk[F[_], K, V](
  rdd: RDD[NJConsumerRecord[K, V]],
  kit: KafkaTopicKit[K, V],
  cfg: SKConfig)(implicit sparkSession: SparkSession)
    extends SparKafkaUpdateParams[FsmRddDisk[F, K, V]] {
  override def params: SKParams = SKConfigF.evalConfig(cfg)

  override def withParamUpdate(f: SKConfig => SKConfig): FsmRddDisk[F, K, V] =
    new FsmRddDisk[F, K, V](rdd, kit, f(cfg))

  def crDataset(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmConsumerRecords[F, K, V] = {
    val tds       = TypedDataset.create(rdd)
    val inBetween = tds.makeUDF[Long, Boolean](params.timeRange.isInBetween)
    new FsmConsumerRecords(tds.filter(inBetween(tds('timestamp))).dataset, kit, cfg)
  }

  def partition(num: Int): FsmRddDisk[F, K, V] =
    new FsmRddDisk[F, K, V](rdd.filter(_.partition === num), kit, cfg)

  def sorted: RDD[NJConsumerRecord[K, V]] =
    rdd
      .filter(m => params.timeRange.isInBetween(m.timestamp))
      .repartition(params.repartition.value)
      .sortBy[NJConsumerRecord[K, V]](identity)

  def crStream(implicit F: Sync[F]): Stream[F, NJConsumerRecord[K, V]] =
    Stream.fromIterator[F](sorted.toLocalIterator)

  def count: Long = rdd.count()

  def replay(
    implicit
    concurrentEffect: ConcurrentEffect[F],
    timer: Timer[F],
    contextShift: ContextShift[F]): F[Unit] =
    crStream
      .map(_.toNJProducerRecord.noMeta)
      .chunkN(params.uploadRate.batchSize)
      .metered(params.uploadRate.duration)
      .through(sk.upload(kit))
      .map(_ => print("."))
      .compile
      .drain

  def stats: Statistics[F] =
    new Statistics(TypedDataset.create(rdd.map(CRMetaInfo(_))).dataset, cfg)
}
