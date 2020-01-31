package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJProducerRecord
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import fs2.kafka.ProducerResult
import org.apache.spark.sql.Dataset
import shapeless._

final class FsmProducerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  prs: Dataset[NJProducerRecord[K, V]],
  sks: SparKafkaSession[K,V]
) extends FsmSparKafka {

  @transient lazy val dataset: TypedDataset[NJProducerRecord[K, V]] =
    TypedDataset.create(prs)

  def upload(kit: KafkaTopicKit[K, V])(
    implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    sk.upload(dataset, kit, sks.params.repartition, sks.params.uploadRate)

  def upload(
    implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    upload(sks.kit)

  def show(implicit ev: Sync[F]): F[Unit] =
    dataset.show[F](sks.params.showDs.rowNum, sks.params.showDs.isTruncate)

}
