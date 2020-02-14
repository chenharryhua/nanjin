package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJProducerRecord
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import fs2.kafka.ProducerResult
import org.apache.spark.sql.Dataset

import scala.concurrent.duration.FiniteDuration

final class FsmProducerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  prs: Dataset[NJProducerRecord[K, V]],
  kit: KafkaTopicKit[K, V],
  params: SKConfigParamF.ConfigParam
) extends Serializable {

  // config section
  def withBatchSize(num: Int) =
    new FsmProducerRecords[F, K, V](prs, kit, SKConfigParamF.withBatchSize(num, params))

  def withDuration(fd: FiniteDuration) =
    new FsmProducerRecords[F, K, V](prs, kit, SKConfigParamF.withDuration(fd, params))

  def withRepartition(rp: Int) =
    new FsmProducerRecords[F, K, V](prs, kit, SKConfigParamF.withRepartition(rp, params))

  def noTimestamp: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](
      typedDataset.deserialized.map(_.noTimestamp).dataset,
      kit,
      params)

  def noPartiton: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](
      typedDataset.deserialized.map(_.noPartition).dataset,
      kit,
      params)

  def noMeta: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](typedDataset.deserialized.map(_.noMeta).dataset, kit, params)

  def count(implicit ev: Sync[F]): F[Long] =
    typedDataset.count[F]()

  @transient lazy val typedDataset: TypedDataset[NJProducerRecord[K, V]] =
    TypedDataset.create(prs)

  private val p: SKParams = SKConfigParamF.evalParams(params)

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
