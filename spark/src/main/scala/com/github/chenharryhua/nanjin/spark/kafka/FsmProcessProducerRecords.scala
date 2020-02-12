package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJProducerRecord
import com.github.chenharryhua.nanjin.spark.{NJRepartition, NJShowDataset}
import frameless.{TypedDataset, TypedEncoder}
import frameless.cats.implicits._
import fs2.Stream
import fs2.kafka.ProducerResult
import monocle.macros.Lenses
import org.apache.spark.sql.Dataset

@Lenses final case class ProcessProducerRecordParams(
  uploadRate: UploadRate,
  repartition: NJRepartition,
  showDs: NJShowDataset)

object ProcessProducerRecordParams {

  def apply(showDs: NJShowDataset): ProcessProducerRecordParams =
    ProcessProducerRecordParams(
      uploadRate  = UploadRate.default,
      repartition = NJRepartition(30),
      showDs      = showDs
    )
}

final class FsmProcessProducerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  prs: Dataset[NJProducerRecord[K, V]],
  kit: KafkaTopicKit[K, V],
  params: ProcessProducerRecordParams
) {

  @transient lazy val typedDataset: TypedDataset[NJProducerRecord[K, V]] =
    TypedDataset.create(prs)

  def noTimestamp: FsmProcessProducerRecords[F, K, V] =
    new FsmProcessProducerRecords[F, K, V](
      typedDataset.deserialized.map(_.noTimestamp).dataset,
      kit,
      params)

  def noPartiton: FsmProcessProducerRecords[F, K, V] =
    new FsmProcessProducerRecords[F, K, V](
      typedDataset.deserialized.map(_.noPartition).dataset,
      kit,
      params)

  def noMeta: FsmProcessProducerRecords[F, K, V] =
    new FsmProcessProducerRecords[F, K, V](
      typedDataset.deserialized.map(_.noMeta).dataset,
      kit,
      params)

  def count(implicit ev: Sync[F]): F[Long] =
    typedDataset.count[F]()

  def upload(other: KafkaTopicKit[K, V])(
    implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    sk.upload(typedDataset, other, params.repartition, params.uploadRate)

  def upload(
    implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    upload(kit)

  def show(implicit ev: Sync[F]): F[Unit] =
    typedDataset.show[F](params.showDs.rowNum, params.showDs.isTruncate)

}
