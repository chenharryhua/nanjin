package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.database.{DatabaseName, DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist._
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.{Dataset, SparkSession}
import cats.effect.Sync

final class TableDataset[F[_], A](
  val dataset: Dataset[A],
  dbSettings: DatabaseSettings,
  cfg: STConfig)(implicit ate: AvroTypedEncoder[A])
    extends Serializable {

  import ate.sparkTypedEncoder.classTag
  implicit private val te: TypedEncoder[A] = ate.sparkTypedEncoder
  implicit private val ss: SparkSession    = dataset.sparkSession
  implicit private val ae: NJAvroCodec[A]  = ate.avroCodec

  val params: STParams = cfg.evalConfig

  def repartition(num: Int): TableDataset[F, A] =
    new TableDataset[F, A](dataset.repartition(num), dbSettings, cfg)

  def withPathBuilder(f: (DatabaseName, TableName, NJFileFormat) => String) =
    new TableDataset[F, A](dataset, dbSettings, cfg.withPathBuilder(f))

  def typedDataset: TypedDataset[A] = TypedDataset.create(dataset)

  def count(implicit F: Sync[F]): F[Long] = F.delay(dataset.count())

  def upload: DbUploader[F, A] = new DbUploader[F, A](dataset, dbSettings, ate, cfg)

  def save: RddFileHoarder[F, A] = new RddFileHoarder[F, A](dataset.rdd)

  def partition =
    new RddPartitionHoarder[F, A, Unit](
      dataset.rdd,
      a => Some(()),
      (fmt, _) => params.pathBuilder(dbSettings.database, params.tableName, fmt))

}
