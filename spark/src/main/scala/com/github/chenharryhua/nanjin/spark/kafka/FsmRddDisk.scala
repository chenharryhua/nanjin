package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
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

  def sorted: RDD[NJConsumerRecord[K, V]] =
    rdd
      .filter(m => params.timeRange.isInBetween(m.timestamp))
      .sortBy(_.timestamp)
      .repartition(params.repartition.value)

  def crStream(implicit F: Sync[F]): Stream[F, NJConsumerRecord[K, V]] =
    Stream.fromIterator[F](sorted.toLocalIterator)

  def replay(
    implicit
    concurrentEffect: ConcurrentEffect[F],
    timer: Timer[F],
    contextShift: ContextShift[F]): F[Unit] =
    crStream
      .map(_.toNJProducerRecord.noMeta)
      .chunkN(params.uploadRate.batchSize)
      .metered(params.uploadRate.duration)
      .through(kit.upload)
      .map(_ => print("."))
      .compile
      .drain

}
