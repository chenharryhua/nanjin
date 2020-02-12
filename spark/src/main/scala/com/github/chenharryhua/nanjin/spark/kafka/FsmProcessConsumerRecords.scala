package com.github.chenharryhua.nanjin.spark.kafka

import java.time.ZoneId

import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.github.chenharryhua.nanjin.spark.NJShowDataset
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import monocle.macros.Lenses
import org.apache.spark.sql.{Dataset, SaveMode}

@Lenses final case class ProcessConsumerRecordParams(
  zoneId: ZoneId,
  showDs: NJShowDataset,
  fileFormat: NJFileFormat,
  saveMode: SaveMode,
  path: String
)

object ProcessConsumerRecordParams {

  def apply(zoneId: ZoneId, path: String): ProcessConsumerRecordParams =
    ProcessConsumerRecordParams(
      zoneId     = zoneId,
      showDs     = NJShowDataset(60, isTruncate = false),
      fileFormat = NJFileFormat.Parquet,
      saveMode   = SaveMode.ErrorIfExists,
      path       = path
    )
}

final class FsmProcessConsumerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  crs: Dataset[NJConsumerRecord[K, V]],
  kit: KafkaTopicKit[K, V],
  params: ProcessConsumerRecordParams) {

  @transient lazy val typedDataset: TypedDataset[NJConsumerRecord[K, V]] =
    TypedDataset.create(crs)

  def bimapTo[K2: TypedEncoder, V2: TypedEncoder](
    other: KafkaTopicKit[K2, V2])(k: K => K2, v: V => V2) =
    new FsmProcessConsumerRecords[F, K2, V2](
      typedDataset.deserialized.map(_.bimap(k, v)).dataset,
      other,
      params)

  def flatMapTo[K2: TypedEncoder, V2: TypedEncoder](other: KafkaTopicKit[K2, V2])(
    f: NJConsumerRecord[K, V] => TraversableOnce[NJConsumerRecord[K2, V2]]) =
    new FsmProcessConsumerRecords[F, K2, V2](
      typedDataset.deserialized.flatMap(f).dataset,
      other,
      params)

  def someValues =
    new FsmProcessConsumerRecords[F, K, V](
      typedDataset.filter(typedDataset('value).isNotNone).dataset,
      kit,
      params)

  def filter(f: NJConsumerRecord[K, V] => Boolean): FsmProcessConsumerRecords[F, K, V] =
    new FsmProcessConsumerRecords[F, K, V](crs.filter(f), kit, params)

  def persist: FsmProcessConsumerRecords[F, K, V] =
    new FsmProcessConsumerRecords[F, K, V](crs.persist(), kit, params)

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

  def save(): Unit = save(params.path)

  def toProducerRecords: FsmProcessProducerRecords[F, K, V] =
    new FsmProcessProducerRecords(
      (typedDataset.deserialized.map(_.toNJProducerRecord)).dataset,
      kit,
      ProcessProducerRecordParams(params.showDs))

  def stats: Statistics[F, K, V] =
    new Statistics(crs, params.showDs, params.zoneId)
}
