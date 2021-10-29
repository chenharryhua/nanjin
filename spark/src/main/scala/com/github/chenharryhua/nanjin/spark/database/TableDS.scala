package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.database.DatabaseSettings
import com.github.chenharryhua.nanjin.spark.persist.DatasetAvroFileHoarder
import frameless.TypedDataset
import org.apache.spark.sql.Dataset

final class TableDS[F[_], A] private[database] (
  val dataset: Dataset[A],
  tableDef: TableDef[A],
  dbSettings: DatabaseSettings,
  cfg: STConfig)
    extends Serializable {

  val params: STParams = cfg.evalConfig

  def transform(f: Dataset[A] => Dataset[A]): TableDS[F, A] =
    new TableDS[F, A](f(dataset), tableDef, dbSettings, cfg)

  def normalize: TableDS[F, A] = transform(tableDef.avroTypedEncoder.normalize(_).dataset)

  def repartition(num: Int): TableDS[F, A] = transform(_.repartition(num))

  def map[B](f: A => B)(tdb: TableDef[B]): TableDS[F, B] =
    new TableDS[F, B](dataset.map(f)(tdb.avroTypedEncoder.sparkEncoder), tdb, dbSettings, cfg).normalize

  def flatMap[B](f: A => IterableOnce[B])(tdb: TableDef[B]): TableDS[F, B] =
    new TableDS[F, B](dataset.flatMap(f)(tdb.avroTypedEncoder.sparkEncoder), tdb, dbSettings, cfg).normalize

  def typedDataset: TypedDataset[A] = TypedDataset.create(dataset)(tableDef.avroTypedEncoder.typedEncoder)

  def upload: DbUploader[F, A] =
    new DbUploader[F, A](dataset, dbSettings, cfg)

  def save: DatasetAvroFileHoarder[F, A] =
    new DatasetAvroFileHoarder[F, A](dataset, tableDef.avroTypedEncoder.avroCodec.avroEncoder)

}
