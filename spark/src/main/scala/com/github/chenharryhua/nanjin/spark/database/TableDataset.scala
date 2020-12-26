package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.database.DatabaseSettings
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.DatasetAvroFileHoarder
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel

final class TableDataset[F[_], A] private[database] (
  val dataset: Dataset[A],
  dbSettings: DatabaseSettings,
  cfg: STConfig,
  ate: AvroTypedEncoder[A])
    extends Serializable {

  val params: STParams = cfg.evalConfig

  def transform(f: Dataset[A] => Dataset[A]): TableDataset[F, A] =
    new TableDataset[F, A](f(dataset), dbSettings, cfg, ate)

  def normalize: TableDataset[F, A] = transform(ate.normalize(_).dataset)

  def repartition(num: Int): TableDataset[F, A] = transform(_.repartition(num))

  def persist(level: StorageLevel): TableDataset[F, A] = transform(_.persist(level))
  def unpersist: TableDataset[F, A]                    = transform(_.unpersist())

  def map[B](f: A => B)(ateb: AvroTypedEncoder[B]): TableDataset[F, B] =
    new TableDataset[F, B](dataset.map(f)(ateb.sparkEncoder), dbSettings, cfg, ateb).normalize

  def flatMap[B](f: A => TraversableOnce[B])(ateb: AvroTypedEncoder[B]): TableDataset[F, B] =
    new TableDataset[F, B](dataset.flatMap(f)(ateb.sparkEncoder), dbSettings, cfg, ateb).normalize

  def typedDataset: TypedDataset[A] = TypedDataset.create(dataset)(ate.typedEncoder)

  def upload: DbUploader[F, A] =
    new DbUploader[F, A](dataset, dbSettings, ate, cfg)

  def save: DatasetAvroFileHoarder[F, A] =
    new DatasetAvroFileHoarder[F, A](dataset, ate.avroCodec.avroEncoder)

}
