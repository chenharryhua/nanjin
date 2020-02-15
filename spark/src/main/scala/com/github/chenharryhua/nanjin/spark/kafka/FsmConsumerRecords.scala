package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class FsmConsumerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  crs: Dataset[NJConsumerRecord[K, V]],
  kit: KafkaTopicKit[K, V],
  cfg: SKConfig)
    extends SparKafkaUpdateParams[FsmConsumerRecords[F, K, V]] {

  override def withParamUpdate(f: SKConfig => SKConfig): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs, kit, f(cfg))

  @transient lazy val typedDataset: TypedDataset[NJConsumerRecord[K, V]] =
    TypedDataset.create(crs)

  override val params: SKParams = SKConfigF.evalConfig(cfg)

  // api section
  def bimapTo[K2: TypedEncoder, V2: TypedEncoder](
    other: KafkaTopicKit[K2, V2])(k: K => K2, v: V => V2) =
    new FsmConsumerRecords[F, K2, V2](
      typedDataset.deserialized.map(_.bimap(k, v)).dataset,
      other,
      cfg)

  def flatMapTo[K2: TypedEncoder, V2: TypedEncoder](other: KafkaTopicKit[K2, V2])(
    f: NJConsumerRecord[K, V] => TraversableOnce[NJConsumerRecord[K2, V2]]) =
    new FsmConsumerRecords[F, K2, V2](typedDataset.deserialized.flatMap(f).dataset, other, cfg)

  def someValues =
    new FsmConsumerRecords[F, K, V](
      typedDataset.filter(typedDataset('value).isNotNone).dataset,
      kit,
      cfg)

  def filter(f: NJConsumerRecord[K, V] => Boolean): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs.filter(f), kit, cfg)

  def persist: FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs.persist(), kit, cfg)

  def nullValuesCount(implicit F: Sync[F]): F[Long] =
    typedDataset.filter(typedDataset('value).isNone).count[F]

  def nullKeysCount(implicit ev: Sync[F]): F[Long] =
    typedDataset.filter(typedDataset('key).isNone).count[F]

  def count(implicit ev: Sync[F]): F[Long] = typedDataset.count[F]()

  def values: TypedDataset[V] =
    typedDataset.select(typedDataset('value)).as[Option[V]].deserialized.flatMap[V](identity)

  def keys: TypedDataset[K] =
    typedDataset.select(typedDataset('key)).as[Option[K]].deserialized.flatMap[K](identity)

  def show(implicit ev: Sync[F]): F[Unit] =
    typedDataset.show[F](params.showDs.rowNum, params.showDs.isTruncate)

  def save(path: String): Unit =
    sk.save(typedDataset, kit, params.fileFormat, params.saveMode, path)

  def save(): Unit = save(params.pathBuilder(NJPathBuild(params.fileFormat, kit.topicName)))

  def toProducerRecords: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords((typedDataset.deserialized.map(_.toNJProducerRecord)).dataset, kit, cfg)

  def stats: Statistics[F, K, V] = new Statistics(crs, cfg)
}
