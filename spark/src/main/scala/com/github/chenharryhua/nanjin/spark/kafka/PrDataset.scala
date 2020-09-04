package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerRecord
import com.github.chenharryhua.nanjin.spark._
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import fs2.kafka.ProducerResult
import org.apache.spark.sql.Dataset

final class PrDataset[F[_], K, V](
  prs: Dataset[NJProducerRecord[K, V]],
  codec: KafkaAvroTypedEncoder[K, V],
  cfg: SKConfig
) extends SparKafkaUpdateParams[PrDataset[F, K, V]] {

  implicit private val prEncoder: TypedEncoder[NJProducerRecord[K, V]] = codec.producerRecord
  implicit private val valTypedEncoder: TypedEncoder[V]                = codec.valEncoder

  override val params: SKParams = cfg.evalConfig

  override def withParamUpdate(f: SKConfig => SKConfig): PrDataset[F, K, V] =
    new PrDataset[F, K, V](prs, codec, f(cfg))

  @transient lazy val typedDataset: TypedDataset[NJProducerRecord[K, V]] =
    TypedDataset.create(prs)

  def noTimestamp: PrDataset[F, K, V] =
    new PrDataset[F, K, V](typedDataset.deserialized.map(_.noTimestamp).dataset, codec, cfg)

  def noPartition: PrDataset[F, K, V] =
    new PrDataset[F, K, V](typedDataset.deserialized.map(_.noPartition).dataset, codec, cfg)

  def noMeta: PrDataset[F, K, V] =
    new PrDataset[F, K, V](typedDataset.deserialized.map(_.noMeta).dataset, codec, cfg)

  def someValues: PrDataset[F, K, V] =
    new PrDataset[F, K, V](typedDataset.filter(typedDataset('value).isNotNone).dataset, codec, cfg)

  // actions
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

  def count(implicit ev: Sync[F]): F[Long] =
    typedDataset.count[F]()

  def show(implicit ev: Sync[F]): F[Unit] =
    typedDataset.show[F](params.showDs.rowNum, params.showDs.isTruncate)
}
