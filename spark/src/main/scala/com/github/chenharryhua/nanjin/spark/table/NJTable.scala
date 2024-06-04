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

final class NJTable[A] private[spark] (val dataset: Dataset[A], ate: AvroTypedEncoder[A]) {

  // transform

  def map[B](bate: AvroTypedEncoder[B])(f: A => B): NJTable[B] =
    new NJTable[B](dataset.map(f)(bate.sparkEncoder), bate)

  def flatMap[B](bate: AvroTypedEncoder[B])(f: A => IterableOnce[B]): NJTable[B] =
    new NJTable[B](dataset.flatMap(f)(bate.sparkEncoder), bate)

  def transform(f: Endo[Dataset[A]]): NJTable[A] = new NJTable[A](f(dataset), ate)

  def repartition(numPartitions: Int): NJTable[A] = transform(_.repartition(numPartitions))

  def normalize: NJTable[A] = transform(ate.normalize)

  def diff(other: Dataset[A]): NJTable[A] = transform(_.except(other))
  def diff(other: NJTable[A]): NJTable[A] = diff(other.dataset)

  def union(other: Dataset[A]): NJTable[A] = transform(_.union(other))
  def union(other: NJTable[A]): NJTable[A] = union(other.dataset)

  // transition

  def output: RddAvroFileHoarder[A] = new RddAvroFileHoarder[A](dataset.rdd, ate.avroCodec)

  // IO

  def stream[F[_]: Sync](chunkSize: ChunkSize): Stream[F, A] =
    Stream.fromBlockingIterator[F](dataset.toLocalIterator().asScala, chunkSize.value)

  def count[F[_]](implicit F: Sync[F]): F[Long] = F.blocking(dataset.count())

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

object NJTable {
  def empty[A](ate: AvroTypedEncoder[A], ss: SparkSession): NJTable[A] =
    new NJTable[A](ate.emptyDataset(ss), ate)
}
