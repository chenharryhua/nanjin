package com.github.chenharryhua.nanjin.spark.table

import cats.{Endo, Monad}
import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.RddAvroFileHoarder
import com.zaxxer.hikari.HikariConfig
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

final class NJTable[F[_]: Monad, A](val fdataset: F[Dataset[A]], ate: AvroTypedEncoder[A]) {

  // lazy val typedDataset: TypedDataset[A] = TypedDataset.create(fdataset)(ate.typedEncoder)

  def map[B](bate: AvroTypedEncoder[B])(f: A => B): NJTable[F, B] =
    new NJTable[F, B](fdataset.map(_.map(f)(bate.sparkEncoder)), bate)

  def flatMap[B](bate: AvroTypedEncoder[B])(f: A => IterableOnce[B]): NJTable[F, B] =
    new NJTable[F, B](fdataset.map(_.flatMap(f)(bate.sparkEncoder)), bate)

  def transform(f: Endo[Dataset[A]]): NJTable[F, A] = new NJTable[F, A](fdataset.map(f), ate)

  def repartition(numPartitions: Int): NJTable[F, A] = transform(_.repartition(numPartitions))

  def normalize: NJTable[F, A] = transform(ate.normalize)

  def diff(other: Dataset[A]): NJTable[F, A] = transform(_.except(other))
  def diff(other: NJTable[F, A]): NJTable[F, A] =
    new NJTable[F, A](fdataset.flatMap(me => other.fdataset.map(me.except)), ate)

  def union(other: Dataset[A]): NJTable[F, A] = transform(_.union(other))
  def union(other: NJTable[F, A]): NJTable[F, A] =
    new NJTable[F, A](other.fdataset.flatMap(o => fdataset.map(_.union(o))), ate)

  def output: RddAvroFileHoarder[F, A] =
    new RddAvroFileHoarder[F, A](fdataset.map(_.rdd), ate.avroCodec.avroEncoder)

  def count: F[Long] = fdataset.map(_.count())

  def upload(hikariConfig: HikariConfig, tableName: TableName, saveMode: SaveMode)(implicit
    F: Sync[F]): F[Unit] =
    F.flatMap(fdataset)(ds =>
      F.blocking(
        ds.write
          .mode(saveMode)
          .format("jdbc")
          .option("driver", hikariConfig.getDriverClassName)
          .option("url", hikariConfig.getJdbcUrl)
          .option("user", hikariConfig.getUsername)
          .option("password", hikariConfig.getPassword)
          .option("dbtable", tableName.value)
          .save()))
}

object NJTable {
  def empty[F[_]: Monad, A](ate: AvroTypedEncoder[A], ss: SparkSession): NJTable[F, A] =
    new NJTable[F, A](Monad[F].pure(ate.emptyDataset(ss)), ate)
}
