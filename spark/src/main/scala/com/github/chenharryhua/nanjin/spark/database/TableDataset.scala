package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.database.DatabaseSettings
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.DatasetAvroFileHoarder
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.ClassTag

final class TableDataset[F[_], A] private[database] (
  val dataset: Dataset[A],
  dbSettings: DatabaseSettings,
  cfg: STConfig,
  ate: AvroTypedEncoder[A])
    extends Serializable {

  implicit private val classTag: ClassTag[A] = ate.classTag
  implicit private val ss: SparkSession      = dataset.sparkSession

  val params: STParams = cfg.evalConfig

  def repartition(num: Int): TableDataset[F, A] =
    new TableDataset[F, A](dataset.repartition(num), dbSettings, cfg, ate)

  def map[B](f: A => B)(ateb: AvroTypedEncoder[B]): TableDataset[F, B] =
    new TableDataset[F, B](dataset.map(f)(ateb.sparkEncoder), dbSettings, cfg, ateb)

  def flatMap[B](f: A => TraversableOnce[B])(ateb: AvroTypedEncoder[B]): TableDataset[F, B] =
    new TableDataset[F, B](dataset.flatMap(f)(ateb.sparkEncoder), dbSettings, cfg, ateb)

  def typedDataset: TypedDataset[A] = TypedDataset.create(dataset)(ate.typedEncoder)

  def upload: DbUploader[F, A] = new DbUploader[F, A](dataset, dbSettings, ate, cfg)

  def save: DatasetAvroFileHoarder[F, A] =
    new DatasetAvroFileHoarder[F, A](dataset, ate.avroCodec.avroEncoder)

}
