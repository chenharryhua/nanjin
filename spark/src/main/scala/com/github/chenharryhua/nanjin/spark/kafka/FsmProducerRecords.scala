package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJProducerRecord
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import fs2.kafka.ProducerResult
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

  def noPartiton: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](typedDataset.deserialized.map(_.noPartition).dataset, kit, cfg)

  def noMeta: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](typedDataset.deserialized.map(_.noMeta).dataset, kit, cfg)

  def count(implicit ev: Sync[F]): F[Long] =
    typedDataset.count[F]()

  @transient lazy val typedDataset: TypedDataset[NJProducerRecord[K, V]] =
    TypedDataset.create(prs)

  private val p: SKParams = SKConfigF.evalConfig(cfg)

  // api section
  def upload(other: KafkaTopicKit[K, V])(
    implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    sk.upload(typedDataset, other, p.repartition, p.uploadRate)

  def upload(
    implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    upload(kit)

  def show(implicit ev: Sync[F]): F[Unit] =
    typedDataset.show[F](p.showDs.rowNum, p.showDs.isTruncate)

}
