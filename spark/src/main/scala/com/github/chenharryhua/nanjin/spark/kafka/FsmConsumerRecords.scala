package com.github.chenharryhua.nanjin.spark.kafka

import java.time.ZoneId

import cats.effect.Sync
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class FsmConsumerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  ds: Dataset[NJConsumerRecord[K, V]],
  sks: SparKafkaSession[K, V])
    extends FsmSparKafka {

  @transient lazy val dataset: TypedDataset[NJConsumerRecord[K, V]] =
    TypedDataset.create(ds)

  def nullValuesCount(implicit ev: Sync[F]): F[Long] =
    dataset.filter(dataset('value).isNone).count[F]

  def nullKeysCount(implicit ev: Sync[F]): F[Long] =
    dataset.filter(dataset('key).isNone).count[F]

  def values: TypedDataset[V] =
    dataset.select(dataset('value)).as[Option[V]].deserialized.flatMap(x => x)

  def keys: TypedDataset[K] =
    dataset.select(dataset('key)).as[Option[K]].deserialized.flatMap(x => x)

  def show(implicit ev: Sync[F]): F[Unit] =
    dataset.show[F](sks.params.showDs.rowNum, sks.params.showDs.isTruncate)

  def save(fileFormat: NJFileFormat, path: String): Unit =
    sk.save(
      dataset,
      sks.kit,
      sks.params.fileFormat,
      sks.params.saveMode,
      sks.params.getPath(sks.kit.topicName))

  def save(): Unit = save(sks.params.fileFormat, sks.params.getPath(sks.kit.topicName))

  def toProducerRecords(conversionTactics: ConversionTactics): FsmProducerRecords[F, K, V] =
    sks.prDataset(sk.cr2pr(dataset, conversionTactics, sks.params.clock))

  def toProducerRecords: FsmProducerRecords[F, K, V] =
    toProducerRecords(sks.params.conversionTactics)

  def stats(zoneId: ZoneId): FsmStatistics[F, K, V] = new FsmStatistics(ds, zoneId)
  def stats: FsmStatistics[F, K, V]                 = stats(sks.params.zoneId)
}
