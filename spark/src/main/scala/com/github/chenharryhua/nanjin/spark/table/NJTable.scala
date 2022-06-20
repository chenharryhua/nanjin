package com.github.chenharryhua.nanjin.spark.table

import cats.Endo
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.{RddAvroFileHoarder, RddStreamSource}
import com.zaxxer.hikari.HikariConfig
import frameless.TypedDataset
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

final class NJTable[F[_], A](val dataset: Dataset[A], ate: AvroTypedEncoder[A]) {

  lazy val typedDataset: TypedDataset[A] = TypedDataset.create(dataset)(ate.typedEncoder)

  def map[B](bate: AvroTypedEncoder[B])(f: A => B): NJTable[F, B] =
    new NJTable[F, B](dataset.map(f)(bate.sparkEncoder), bate)

  def flatMap[B](bate: AvroTypedEncoder[B])(f: A => IterableOnce[B]): NJTable[F, B] =
    new NJTable[F, B](dataset.flatMap(f)(bate.sparkEncoder), bate)

  def transform(f: Endo[Dataset[A]]): NJTable[F, A] = new NJTable[F, A](f(dataset), ate)

  def normalize: NJTable[F, A] = transform(ate.normalize)

  def diff(other: Dataset[A]): NJTable[F, A]    = transform(_.except(other))
  def diff(other: NJTable[F, A]): NJTable[F, A] = diff(other.dataset)

  def union(other: Dataset[A]): NJTable[F, A]    = transform(_.union(other))
  def union(other: NJTable[F, A]): NJTable[F, A] = union(other.dataset)

  def save: RddAvroFileHoarder[F, A] =
    new RddAvroFileHoarder[F, A](dataset.rdd, ate.avroCodec.avroEncoder)

  def asSource: RddStreamSource[F, A] = new RddStreamSource[F, A](dataset.rdd)

  def count(implicit F: Sync[F]): F[Long] = F.delay(dataset.count())

  def upload(hikariConfig: HikariConfig, tableName: TableName, saveMode: SaveMode)(implicit
    F: Sync[F]): F[Unit] =
    F.delay {
      dataset.write
        .mode(saveMode)
        .format("jdbc")
        .option("driver", hikariConfig.getDriverClassName)
        .option("url", hikariConfig.getJdbcUrl)
        .option("user", hikariConfig.getUsername)
        .option("password", hikariConfig.getPassword)
        .option("dbtable", tableName.value)
        .save()
    }
}

object NJTable {
  def empty[F[_], A](ate: AvroTypedEncoder[A], ss: SparkSession): NJTable[F, A] =
    new NJTable[F, A](ate.emptyDataset(ss), ate)
}
