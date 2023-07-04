package com.github.chenharryhua.nanjin.spark.table

import cats.Endo
import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.RddAvroFileHoarder
import com.zaxxer.hikari.HikariConfig
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

final class NJTable[F[_], A](val fdataset: F[Dataset[A]], ate: AvroTypedEncoder[A])(implicit F: Sync[F]) {

  def map[B](bate: AvroTypedEncoder[B])(f: A => B): NJTable[F, B] =
    new NJTable[F, B](F.flatMap(fdataset)(ds => F.blocking(ds.map(f)(bate.sparkEncoder))), bate)

  def flatMap[B](bate: AvroTypedEncoder[B])(f: A => IterableOnce[B]): NJTable[F, B] =
    new NJTable[F, B](F.flatMap(fdataset)(ds => F.blocking(ds.flatMap(f)(bate.sparkEncoder))), bate)

  def transform(f: Endo[Dataset[A]]): NJTable[F, A] =
    new NJTable[F, A](F.flatMap(fdataset)(ds => F.blocking(f(ds))), ate)

  def repartition(numPartitions: Int): NJTable[F, A] = transform(_.repartition(numPartitions))
  def normalize: NJTable[F, A]                       = transform(ate.normalize)

  def diff(other: Dataset[A]): NJTable[F, A] = transform(_.except(other))
  def diff(other: NJTable[F, A]): NJTable[F, A] = {
    val ds = for {
      me <- fdataset
      you <- other.fdataset
      ds <- F.blocking(me.except(you))
    } yield ds
    new NJTable[F, A](ds, ate)
  }

  def union(other: Dataset[A]): NJTable[F, A] = transform(_.union(other))
  def union(other: NJTable[F, A]): NJTable[F, A] = {
    val ds = for {
      me <- fdataset
      you <- other.fdataset
      ds <- F.blocking(me.union(you))
    } yield ds
    new NJTable[F, A](ds, ate)
  }

  def output: RddAvroFileHoarder[F, A] =
    new RddAvroFileHoarder[F, A](F.flatMap(fdataset)(ds => F.blocking(ds.rdd)), ate.avroCodec.avroEncoder)

  def count: F[Long] = F.flatMap(fdataset)(ds => F.interruptible(ds.count()))

  def upload(hikariConfig: HikariConfig, tableName: TableName, saveMode: SaveMode): F[Unit] =
    F.flatMap(fdataset)(ds =>
      F.interruptible(
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
  def empty[F[_]: Sync, A](ate: AvroTypedEncoder[A], ss: SparkSession): NJTable[F, A] =
    new NJTable[F, A](Sync[F].blocking(ate.emptyDataset(ss)), ate)
}
