package com.github.chenharryhua.nanjin.spark.database

import cats.Endo
import com.github.chenharryhua.nanjin.spark.persist.DatasetAvroFileHoarder
import com.zaxxer.hikari.HikariConfig
import frameless.TypedDataset
import org.apache.spark.sql.Dataset

final class TableDS[F[_], A] private[database] (
  val dataset: Dataset[A],
  tableDef: TableDef[A],
  hikariConfig: HikariConfig,
  cfg: STConfig)
    extends Serializable {

  val params: STParams = cfg.evalConfig

  def transform(f: Endo[Dataset[A]]): TableDS[F, A] =
    new TableDS[F, A](f(dataset), tableDef, hikariConfig, cfg)

  def normalize: TableDS[F, A] = transform(tableDef.ate.normalize)

  def repartition(num: Int): TableDS[F, A] = transform(_.repartition(num))

  def map[B](f: A => B)(tdb: TableDef[B]): TableDS[F, B] =
    new TableDS[F, B](dataset.map(f)(tdb.ate.sparkEncoder), tdb, hikariConfig, cfg).normalize

  def flatMap[B](f: A => IterableOnce[B])(tdb: TableDef[B]): TableDS[F, B] =
    new TableDS[F, B](dataset.flatMap(f)(tdb.ate.sparkEncoder), tdb, hikariConfig, cfg).normalize

  def typedDataset: TypedDataset[A] = TypedDataset.create(dataset)(tableDef.ate.typedEncoder)

  def upload: DbUploader[F, A] =
    new DbUploader[F, A](dataset, hikariConfig, cfg)

  def save: DatasetAvroFileHoarder[F, A] =
    new DatasetAvroFileHoarder[F, A](dataset, tableDef.ate.avroCodec.avroEncoder)

}
