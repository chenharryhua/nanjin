package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJProducerRecord
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import fs2.kafka.ProducerResult
import org.apache.spark.sql.Dataset

final class FsmProducerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  prs: Dataset[NJProducerRecord[K, V]],
  bundle: KitBundle[K, V]
) extends FsmSparKafka[K, V] {

  override def withParamUpdate(f: KitBundle[K, V] => KitBundle[K, V]): FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](prs, f(bundle))

  @transient lazy val typedDataset: TypedDataset[NJProducerRecord[K, V]] =
    TypedDataset.create(prs)

  def noTimestamp: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](typedDataset.deserialized.map(_.noTimestamp).dataset, bundle)

  def noPartiton: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](typedDataset.deserialized.map(_.noPartition).dataset, bundle)

  def noMeta: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](typedDataset.deserialized.map(_.noMeta).dataset, bundle)

  def bimapTo[K2: TypedEncoder, V2: TypedEncoder](
    other: KafkaTopicKit[K2, V2])(k: K => K2, v: V => V2): FsmProducerRecords[F, K2, V2] =
    new FsmProducerRecords[F, K2, V2](
      typedDataset.deserialized.map(_.bimap(k, v)).dataset,
      KitBundle(other, bundle.params))

  def flatMapTo[K2: TypedEncoder, V2: TypedEncoder](other: KafkaTopicKit[K2, V2])(
    f: NJProducerRecord[K, V] => TraversableOnce[NJProducerRecord[K2, V2]])
    : FsmProducerRecords[F, K2, V2] =
    new FsmProducerRecords[F, K2, V2](
      typedDataset.deserialized.flatMap(f).dataset,
      KitBundle(other, bundle.params))

  def someValues: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](
      typedDataset.filter(typedDataset('value).isNotNone).dataset,
      bundle)

  def filter(f: NJProducerRecord[K, V] => Boolean): FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](prs.filter(f), bundle)

  def count(implicit ev: Sync[F]): F[Long] =
    typedDataset.count[F]()

  def upload(kit: KafkaTopicKit[K, V])(
    implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    sk.upload(typedDataset, kit, bundle.params.repartition, bundle.params.uploadRate)

  def upload(
    implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    upload(bundle.kit)

  def show(implicit ev: Sync[F]): F[Unit] =
    typedDataset.show[F](bundle.params.showDs.rowNum, bundle.params.showDs.isTruncate)
}
