package com.github.chenharryhua.nanjin.spark.table

import cats.Endo
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.RddAvroFileHoarder
import com.zaxxer.hikari.HikariConfig
import fs2.Stream
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.jdk.CollectionConverters.IteratorHasAsScala

final class Table[A] private[spark] (val dataset: Dataset[A], ate: AvroTypedEncoder[A]) {

  // transform

  def map[B](bate: AvroTypedEncoder[B])(f: A => B): Table[B] =
    new Table[B](dataset.map(f)(bate.sparkEncoder), bate)

  def flatMap[B](bate: AvroTypedEncoder[B])(f: A => IterableOnce[B]): Table[B] =
    new Table[B](dataset.flatMap(f)(bate.sparkEncoder), bate)

  def transform(f: Endo[Dataset[A]]): Table[A] = new Table[A](f(dataset), ate)

  def repartition(numPartitions: Int): Table[A] = transform(_.repartition(numPartitions))

  def normalize: Table[A] = transform(ate.normalize)

  def diff(other: Dataset[A]): Table[A] = transform(_.except(other))
  def diff(other: Table[A]): Table[A]   = diff(other.dataset)

  def union(other: Dataset[A]): Table[A] = transform(_.union(other))
  def union(other: Table[A]): Table[A]   = union(other.dataset)

  // transition

  def output: RddAvroFileHoarder[A] = new RddAvroFileHoarder[A](dataset.rdd, ate.avroCodec)

  // IO

  def stream[F[_]: Sync](chunkSize: ChunkSize): Stream[F, A] =
    Stream.fromBlockingIterator[F](dataset.toLocalIterator().asScala, chunkSize.value)

  def count[F[_]](implicit F: Sync[F]): F[Long] = F.interruptible(dataset.count())

  def upload[F[_]](hikariConfig: HikariConfig, tableName: TableName, saveMode: SaveMode)(implicit
    F: Sync[F]): F[Unit] =
    F.blocking(
      dataset.write
        .mode(saveMode)
        .format("jdbc")
        .option("driver", hikariConfig.getDriverClassName)
        .option("url", hikariConfig.getJdbcUrl)
        .option("user", hikariConfig.getUsername)
        .option("password", hikariConfig.getPassword)
        .option("dbtable", tableName.value)
        .save())
}

object Table {
  def empty[A](ate: AvroTypedEncoder[A], ss: SparkSession): Table[A] =
    new Table[A](ate.emptyDataset(ss), ate)
}
