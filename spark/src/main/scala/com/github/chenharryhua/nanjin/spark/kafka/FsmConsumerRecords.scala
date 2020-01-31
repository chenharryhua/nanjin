package com.github.chenharryhua.nanjin.spark.kafka

import java.time.ZoneId

import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.common.{NJFileFormat, UpdateParams}
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class FsmConsumerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  ds: Dataset[NJConsumerRecord[K, V]],
  bundle: KitBundle[K, V])
    extends FsmSparKafka with UpdateParams[KitBundle[K, V], FsmConsumerRecords[F, K, V]] {

  override def withParamUpdate(f: KitBundle[K, V] => KitBundle[K, V]): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](ds, f(bundle))

  @transient lazy val dataset: TypedDataset[NJConsumerRecord[K, V]] =
    TypedDataset.create(ds)

  def transform[K2: TypedEncoder, V2: TypedEncoder](
    other: KafkaTopicKit[K2, V2])(k: K => K2, v: V => V2): FsmConsumerRecords[F, K2, V2] =
    new FsmConsumerRecords[F, K2, V2](
      dataset.deserialized.map(_.bimap(k, v)).dataset,
      KitBundle(other, bundle.params))

  def nullValuesCount(implicit ev: Sync[F]): F[Long] =
    dataset.filter(dataset('value).isNone).count[F]

  def nullKeysCount(implicit ev: Sync[F]): F[Long] =
    dataset.filter(dataset('key).isNone).count[F]

  def values: TypedDataset[V] =
    dataset.select(dataset('value)).as[Option[V]].deserialized.flatMap(x => x)

  def keys: TypedDataset[K] =
    dataset.select(dataset('key)).as[Option[K]].deserialized.flatMap(x => x)

  def show(implicit ev: Sync[F]): F[Unit] =
    dataset.show[F](bundle.params.showDs.rowNum, bundle.params.showDs.isTruncate)

  def save(fileFormat: NJFileFormat, path: String): Unit =
    sk.save(dataset, bundle.kit, bundle.params.fileFormat, bundle.params.saveMode, bundle.getPath)

  def save(): Unit = save(bundle.params.fileFormat, bundle.getPath)

  def toProducerRecords(conversionTactics: ConversionTactics): FsmProducerRecords[F, K, V] =
    new FsmProducerRecords(sk.cr2pr(dataset, conversionTactics, bundle.clock).dataset, bundle)

  def toProducerRecords: FsmProducerRecords[F, K, V] =
    toProducerRecords(bundle.params.conversionTactics)

  def stats(zoneId: ZoneId): FsmStatistics[F, K, V] = new FsmStatistics(ds, zoneId)
  def stats: FsmStatistics[F, K, V]                 = stats(bundle.zoneId)
}
