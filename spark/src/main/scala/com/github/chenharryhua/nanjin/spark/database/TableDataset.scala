package com.github.chenharryhua.nanjin.spark.database

import cats.effect.Sync
import com.github.chenharryhua.nanjin.database.DatabaseSettings
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist._
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.ClassTag

final class TableDataset[F[_], A](
  val dataset: Dataset[A],
  dbSettings: DatabaseSettings,
  cfg: STConfig,
  ate: AvroTypedEncoder[A])
    extends Serializable {

  implicit private val classTag: ClassTag[A] = ate.classTag
  implicit private val ss: SparkSession      = dataset.sparkSession
  implicit private val ae: AvroCodec[A]      = ate.avroCodec

  val params: STParams = cfg.evalConfig

  def repartition(num: Int): TableDataset[F, A] =
    new TableDataset[F, A](dataset.repartition(num), dbSettings, cfg, ate)

  def map[B](f: A => B)(implicit ev: AvroTypedEncoder[B]): TableDataset[F, B] =
    new TableDataset[F, B](dataset.map(f)(ev.sparkEncoder), dbSettings, cfg, ev)

  def flatMap[B](f: A => TraversableOnce[B])(implicit ev: AvroTypedEncoder[B]): TableDataset[F, B] =
    new TableDataset[F, B](dataset.flatMap(f)(ev.sparkEncoder), dbSettings, cfg, ev)

  def typedDataset: TypedDataset[A] = ate.normalize(dataset)

  def count(implicit F: Sync[F]): F[Long] = F.delay(dataset.count())

  def upload: DbUploader[F, A] = new DbUploader[F, A](dataset, dbSettings, ate, cfg)

  def save: DatabaseSave[F, A] = new DatabaseSave[F, A](dataset, ate)

}
