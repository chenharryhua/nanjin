package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.github.chenharryhua.nanjin.utils.Keyboard
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import fs2.Stream
import fs2.kafka.ProducerRecords
import fs2.kafka.produce

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
    contextShift: ContextShift[F]): F[Unit] = {
    val run: Stream[F, Unit] = for {
      kb <- Keyboard.signal[F]
      _ <- crStream
        .chunkN(params.uploadRate.batchSize)
        .metered(params.uploadRate.duration)
        .pauseWhen(kb.map(_.contains(Keyboard.pauSe)))
        .interruptWhen(kb.map(_.contains(Keyboard.Quit)))
        .map(chk =>
          ProducerRecords(chk.map(_.toNJProducerRecord.noMeta.toFs2ProducerRecord(kit.topicName))))
        .through(produce(kit.fs2ProducerSettings[F]))
        .map(_ => print("."))
    } yield ()
    run.compile.drain
  }
}
