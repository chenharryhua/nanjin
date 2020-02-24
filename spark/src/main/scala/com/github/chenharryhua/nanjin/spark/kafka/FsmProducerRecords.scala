package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJProducerRecord
import com.github.chenharryhua.nanjin.spark._
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import fs2.kafka.{produce, ProducerRecords, ProducerResult}
import org.apache.spark.sql.Dataset

final class FsmProducerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  prs: Dataset[NJProducerRecord[K, V]],
  kit: KafkaTopicKit[K, V],
  cfg: SKConfig
) extends SparKafkaUpdateParams[FsmProducerRecords[F, K, V]] {

  override def withParamUpdate(f: SKConfig => SKConfig): FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](prs, kit, f(cfg))

  def noTimestamp: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](typedDataset.deserialized.map(_.noTimestamp).dataset, kit, cfg)

  def noPartition: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](typedDataset.deserialized.map(_.noPartition).dataset, kit, cfg)

  def noMeta: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](typedDataset.deserialized.map(_.noMeta).dataset, kit, cfg)

  def someValues: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](
      typedDataset.filter(typedDataset('value).isNotNone).dataset,
      kit,
      cfg)

  @transient lazy val typedDataset: TypedDataset[NJProducerRecord[K, V]] =
    TypedDataset.create(prs)

  override val params: SKParams = SKConfigF.evalConfig(cfg)

  def pipeTo[K2, V2](other: KafkaTopicKit[K2, V2])(k: K => K2, v: V => V2)(
    implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K2, V2, Unit]] =
    typedDataset
      .stream[F]
      .map(_.bimap(k, v))
      .chunkN(params.uploadRate.batchSize)
      .metered(params.uploadRate.duration)
      .through(sk.upload(other))

  def upload(other: KafkaTopicKit[K, V])(
    implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    pipeTo(other)(identity, identity)

  def upload(
    implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    upload(kit)

  def count(implicit ev: Sync[F]): F[Long] =
    typedDataset.count[F]()

  def show(implicit ev: Sync[F]): F[Unit] =
    typedDataset.show[F](params.showDs.rowNum, params.showDs.isTruncate)

}
