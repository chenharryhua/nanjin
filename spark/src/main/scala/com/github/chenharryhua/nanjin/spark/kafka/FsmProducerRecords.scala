package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerRecord
import com.github.chenharryhua.nanjin.spark._
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import fs2.kafka.ProducerResult
import org.apache.spark.sql.Dataset

final class FsmProducerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  prs: Dataset[NJProducerRecord[K, V]],
  topic: KafkaTopic[F, K, V],
  cfg: SKConfig
) extends SparKafkaUpdateParams[FsmProducerRecords[F, K, V]] {
  
  override val params: SKParams = cfg.evalConfig

  override def withParamUpdate(f: SKConfig => SKConfig): FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](prs, topic, f(cfg))

  @transient lazy val typedDataset: TypedDataset[NJProducerRecord[K, V]] =
    TypedDataset.create(prs)

  def noTimestamp: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](
      typedDataset.deserialized.map(_.noTimestamp).dataset,
      topic,
      cfg)

  def noPartition: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](
      typedDataset.deserialized.map(_.noPartition).dataset,
      topic,
      cfg)

  def noMeta: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](typedDataset.deserialized.map(_.noMeta).dataset, topic, cfg)

  def someValues: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](
      typedDataset.filter(typedDataset('value).isNotNone).dataset,
      topic,
      cfg)

  def pipeTo[K2, V2](other: KafkaTopic[F, K2, V2])(k: K => K2, v: V => V2)(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K2, V2, Unit]] =
    typedDataset.stream[F].map(_.bimap(k, v)).through(sk.uploader(other, params.uploadRate))

  def upload(other: KafkaTopic[F, K, V])(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    pipeTo(other)(identity, identity)

  def upload(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    upload(topic)

  def count(implicit ev: Sync[F]): F[Long] =
    typedDataset.count[F]()

  def show(implicit ev: Sync[F]): F[Unit] =
    typedDataset.show[F](params.showDs.rowNum, params.showDs.isTruncate)

}
