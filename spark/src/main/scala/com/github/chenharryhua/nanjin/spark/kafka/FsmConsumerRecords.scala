package com.github.chenharryhua.nanjin.spark.kafka

import cats.data.Reader
import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.{Dataset, SaveMode}

final class FsmConsumerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  crs: Dataset[NJConsumerRecord[K, V]],
  kit: KafkaTopicKit[K, V],
  params: SKConfigF.SKConfig)
    extends Serializable {

  // config section
  def withJson: FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs, kit, SKConfigF.withFileFormat(NJFileFormat.Json, params))

  def withJackson: FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](
      crs,
      kit,
      SKConfigF.withFileFormat(NJFileFormat.Jackson, params))

  def withAvro: FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs, kit, SKConfigF.withFileFormat(NJFileFormat.Avro, params))

  def withParquet: FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](
      crs,
      kit,
      SKConfigF.withFileFormat(NJFileFormat.Parquet, params))

  def withPathBuilder(f: NJPathBuild => String): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs, kit, SKConfigF.withPathBuilder(Reader(f), params))

  def withSaveMode(sm: SaveMode): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs, kit, SKConfigF.withSaveMode(sm, params))

  def withOverwrite: FsmConsumerRecords[F, K, V] =
    withSaveMode(SaveMode.Overwrite)

  @transient lazy val typedDataset: TypedDataset[NJConsumerRecord[K, V]] =
    TypedDataset.create(crs)

  private val p: SKParams = SKConfigF.evalParams(params)

  // api section
  def bimapTo[K2: TypedEncoder, V2: TypedEncoder](
    other: KafkaTopicKit[K2, V2])(k: K => K2, v: V => V2) =
    new FsmConsumerRecords[F, K2, V2](
      typedDataset.deserialized.map(_.bimap(k, v)).dataset,
      other,
      params)

  def flatMapTo[K2: TypedEncoder, V2: TypedEncoder](other: KafkaTopicKit[K2, V2])(
    f: NJConsumerRecord[K, V] => TraversableOnce[NJConsumerRecord[K2, V2]]) =
    new FsmConsumerRecords[F, K2, V2](typedDataset.deserialized.flatMap(f).dataset, other, params)

  def someValues =
    new FsmConsumerRecords[F, K, V](
      typedDataset.filter(typedDataset('value).isNotNone).dataset,
      kit,
      params)

  def filter(f: NJConsumerRecord[K, V] => Boolean): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs.filter(f), kit, params)

  def persist: FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs.persist(), kit, params)

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
    typedDataset.show[F](p.showDs.rowNum, p.showDs.isTruncate)

  def save(path: String): Unit =
    sk.save(typedDataset, kit, p.fileFormat, p.saveMode, path)

  def save(): Unit = save(p.pathBuilder(NJPathBuild(p.fileFormat, kit.topicName)))

  def toProducerRecords: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords(
      (typedDataset.deserialized.map(_.toNJProducerRecord)).dataset,
      kit,
      params)

  def stats: Statistics[F, K, V] = new Statistics(crs, params)
}
