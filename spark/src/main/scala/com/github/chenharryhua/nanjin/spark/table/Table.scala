package com.github.chenharryhua.nanjin.spark.table

import cats.Endo
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.spark.persist.RddAvroFileHoarder
import com.github.chenharryhua.nanjin.spark.{describeJob, SchematizedEncoder}
import com.zaxxer.hikari.HikariConfig
import fs2.Stream
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.jdk.CollectionConverters.IteratorHasAsScala

final class Table[A] private[spark] (val dataset: Dataset[A], ate: SchematizedEncoder[A]) {

  // transform

  def map[B](bate: SchematizedEncoder[B])(f: A => B): Table[B] =
    new Table[B](dataset.map(f)(bate.sparkEncoder), bate)

  def flatMap[B](bate: SchematizedEncoder[B])(f: A => IterableOnce[B]): Table[B] =
    new Table[B](dataset.flatMap(f)(bate.sparkEncoder), bate)

  def transform(f: Endo[Dataset[A]]): Table[A] = new Table[A](f(dataset), ate)

  def repartition(numPartitions: Int): Table[A] = transform(_.repartition(numPartitions))
  def persist(f: StorageLevel.type => StorageLevel): Table[A] =
    transform(_.persist(f(StorageLevel)))

  def normalize: Table[A] = transform(ate.normalize)

  def diff(other: Dataset[A]): Table[A] = transform(_.except(other))
  def diff(other: Table[A]): Table[A] = diff(other.dataset)

  def union(other: Dataset[A]): Table[A] = transform(_.union(other))
  def union(other: Table[A]): Table[A] = union(other.dataset)

  // transition

  def output: RddAvroFileHoarder[A] = new RddAvroFileHoarder[A](dataset.rdd, ate.avroCodec)

  // IO

  def stream[F[_]: Sync](chunkSize: ChunkSize): Stream[F, A] =
    Stream.fromBlockingIterator[F](dataset.toLocalIterator().asScala, chunkSize.value)

  def count[F[_]](description: String)(implicit F: Sync[F]): F[Long] =
    describeJob[F](dataset.sparkSession.sparkContext, description).surround(F.delay(dataset.count()))

  def upload[F[_]](hikariConfig: HikariConfig, tableName: TableName, saveMode: SaveMode)(implicit
    F: Sync[F]): F[Unit] =
    describeJob(dataset.sparkSession.sparkContext, s"Upload:${tableName.value}").surround(
      F.delay(
        dataset.write
          .mode(saveMode)
          .format("jdbc")
          .option("driver", hikariConfig.getDriverClassName)
          .option("url", hikariConfig.getJdbcUrl)
          .option("user", hikariConfig.getUsername)
          .option("password", hikariConfig.getPassword)
          .option("dbtable", tableName.value)
          .save()))
}

object Table {
  def empty[A](ate: SchematizedEncoder[A], ss: SparkSession): Table[A] =
    new Table[A](ate.emptyDataset(ss), ate)
}
