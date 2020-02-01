package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class FsmConsumerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  ds: Dataset[NJConsumerRecord[K, V]],
  bundle: KitBundle[K, V])
    extends FsmSparKafka[K, V] {

  override def withParamUpdate(f: KitBundle[K, V] => KitBundle[K, V]): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](ds, f(bundle))

  @transient lazy val typedDataset: TypedDataset[NJConsumerRecord[K, V]] =
    TypedDataset.create(ds)

  def transform[K2: TypedEncoder, V2: TypedEncoder](
    other: KafkaTopicKit[K2, V2])(k: K => K2, v: V => V2): FsmConsumerRecords[F, K2, V2] =
    new FsmConsumerRecords[F, K2, V2](
      typedDataset.deserialized.map(_.bimap(k, v)).dataset,
      KitBundle(other, bundle.params))

  def transform[K2: TypedEncoder, V2: TypedEncoder](other: KafkaTopicKit[K2, V2])(
    f: NJConsumerRecord[K, V] => TraversableOnce[NJConsumerRecord[K2, V2]])
    : FsmConsumerRecords[F, K2, V2] =
    new FsmConsumerRecords[F, K2, V2](
      typedDataset.deserialized.flatMap(f).dataset,
      KitBundle(other, bundle.params))

  def someValues: FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](
      typedDataset.filter(typedDataset('value).isNotNone).dataset,
      bundle)

  def persist: FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](ds.persist(), bundle)

  def nullValuesCount(implicit ev: Sync[F]): F[Long] =
    typedDataset.filter(typedDataset('value).isNone).count[F]

  def nullKeysCount(implicit ev: Sync[F]): F[Long] =
    typedDataset.filter(typedDataset('key).isNone).count[F]

  def values: TypedDataset[V] =
    typedDataset.select(typedDataset('value)).as[Option[V]].deserialized.flatMap(x => x)

  def keys: TypedDataset[K] =
    typedDataset.select(typedDataset('key)).as[Option[K]].deserialized.flatMap(x => x)

  def show(implicit ev: Sync[F]): F[Unit] =
    typedDataset.show[F](bundle.params.showDs.rowNum, bundle.params.showDs.isTruncate)

  def save(path: String): Unit =
    sk.save(typedDataset, bundle.kit, bundle.params.fileFormat, bundle.params.saveMode, path)

  def save(): Unit = save(bundle.getPath)

  def toProducerRecords(conversionTactics: ConversionTactics): FsmProducerRecords[F, K, V] =
    new FsmProducerRecords(sk.cr2pr(typedDataset, conversionTactics, bundle.clock).dataset, bundle)

  def toProducerRecords: FsmProducerRecords[F, K, V] =
    toProducerRecords(bundle.params.conversionTactics)

  def stats: Statistics[F, K, V] =
    new Statistics(ds, bundle.params.showDs, bundle.zoneId)
}
