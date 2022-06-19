package com.github.chenharryhua.nanjin.spark.table

import cats.Endo
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.{RddAvroFileHoarder, RddStreamSource}
import com.zaxxer.hikari.HikariConfig
import frameless.TypedDataset
import org.apache.spark.sql.{Dataset, SaveMode}

final class NJTable[A](val dataset: Dataset[A], ate: AvroTypedEncoder[A]) {

  lazy val typedDataset: TypedDataset[A] = TypedDataset.create(dataset)(ate.typedEncoder)

  def map[B](bate: AvroTypedEncoder[B])(f: A => B): NJTable[B] =
    new NJTable[B](dataset.map(f)(bate.sparkEncoder), bate)

  def flatMap[B](bate: AvroTypedEncoder[B])(f: A => IterableOnce[B]): NJTable[B] =
    new NJTable[B](dataset.flatMap(f)(bate.sparkEncoder), bate)

  def transform(f: Endo[Dataset[A]]): NJTable[A] = new NJTable[A](f(dataset), ate)

  def normalize: NJTable[A] = transform(ate.normalize)

  def diff(other: Dataset[A]): NJTable[A] = transform(_.except(other))
  def diff(other: NJTable[A]): NJTable[A] = diff(other.dataset)

  def union(other: Dataset[A]): NJTable[A] = transform(_.union(other))
  def union(other: NJTable[A]): NJTable[A] = union(other.dataset)

  def save[F[_]]: RddAvroFileHoarder[F, A] =
    new RddAvroFileHoarder[F, A](dataset.rdd, ate.avroCodec.avroEncoder)

  def asSource[F[_]]: RddStreamSource[F, A] = new RddStreamSource[F, A](dataset.rdd)

  def upload[F[_]](hikariConfig: HikariConfig, tableName: TableName, saveMode: SaveMode)(implicit
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
