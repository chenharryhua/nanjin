package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class FsmConsumerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  crs: Dataset[NJConsumerRecord[K, V]],
  cfg: SKConfig)
    extends SparKafkaUpdateParams[FsmConsumerRecords[F, K, V]] {

  override def withParamUpdate(f: SKConfig => SKConfig): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs, f(cfg))

  @transient lazy val typedDataset: TypedDataset[NJConsumerRecord[K, V]] =
    TypedDataset.create(crs)

  override val params: SKParams = cfg.evalConfig

  // transformations
  def bimap[K2: TypedEncoder, V2: TypedEncoder](
    k: K => K2,
    v: V => V2): FsmConsumerRecords[F, K2, V2] =
    new FsmConsumerRecords[F, K2, V2](typedDataset.deserialized.map(_.bimap(k, v)).dataset, cfg)

  def flatMap[K2: TypedEncoder, V2: TypedEncoder](
    f: NJConsumerRecord[K, V] => TraversableOnce[NJConsumerRecord[K2, V2]])
    : FsmConsumerRecords[F, K2, V2] =
    new FsmConsumerRecords[F, K2, V2](typedDataset.deserialized.flatMap(f).dataset, cfg)

  def someValues: FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](
      typedDataset.filter(typedDataset('value).isNotNone).dataset,
      cfg)

  def filter(f: NJConsumerRecord[K, V] => Boolean): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs.filter(f), cfg)

  def ascending: FsmConsumerRecords[F, K, V] = {
    val sd = typedDataset.orderBy(typedDataset('timestamp).asc)
    new FsmConsumerRecords[F, K, V](sd.dataset, cfg)
  }

  def descending: FsmConsumerRecords[F, K, V] = {
    val sd = typedDataset.orderBy(typedDataset('timestamp).desc)
    new FsmConsumerRecords[F, K, V](sd.dataset, cfg)
  }

  def persist: FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs.persist(), cfg)

  // dataset
  def values: TypedDataset[V] =
    typedDataset.select(typedDataset('value)).as[Option[V]].deserialized.flatMap[V](x => x)

  def keys: TypedDataset[K] =
    typedDataset.select(typedDataset('key)).as[Option[K]].deserialized.flatMap[K](x => x)

  // actions
  def nullValuesCount(implicit F: Sync[F]): F[Long] =
    typedDataset.filter(typedDataset('value).isNone).count[F]

  def nullKeysCount(implicit F: Sync[F]): F[Long] =
    typedDataset.filter(typedDataset('key).isNone).count[F]

  def count(implicit F: Sync[F]): F[Long] = typedDataset.count[F]()

  def show(implicit F: Sync[F]): F[Unit] =
    typedDataset.show[F](params.showDs.rowNum, params.showDs.isTruncate)

  // state change
  def toProducerRecords: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords((typedDataset.deserialized.map(_.toNJProducerRecord)).dataset, cfg)

}
