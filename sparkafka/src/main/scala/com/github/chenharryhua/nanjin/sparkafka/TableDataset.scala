package com.github.chenharryhua.nanjin.sparkafka

import cats.effect.{Concurrent, ContextShift, Sync}
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.util.Read
import doobie.util.fragment.Fragment
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import cats.implicits._

final case class TableDef[A](tableName: String)(
  implicit
  val typedEncoder: TypedEncoder[A],
  val doobieRead: Read[A]) {

  def in[F[_]: ContextShift: Concurrent](dbSettings: DatabaseSettings): TableDataset[F, A] =
    TableDataset[F, A](this, dbSettings)
}

final case class TableDataset[F[_]: ContextShift: Concurrent, A](
  tableDef: TableDef[A],
  dbSettings: DatabaseSettings,
  sparkOptions: Map[String, String] = Map.empty,
  saveMode: SaveMode                = SaveMode.Append) {
  import tableDef.{doobieRead, typedEncoder}

  def withSparkOption(key: String, value: String): TableDataset[F, A] =
    copy(sparkOptions = sparkOptions + (key -> value))

  def withSparkOptions(options: Map[String, String]): TableDataset[F, A] =
    copy(sparkOptions = sparkOptions ++ options)

  def withSaveMode(sm: SaveMode): TableDataset[F, A] =
    copy(saveMode = sm)

  // spark
  def datasetFromDB(implicit spark: SparkSession): TypedDataset[A] =
    TypedDataset.createUnsafe[A](
      spark.read
        .format("jdbc")
        .option("url", dbSettings.connStr.value)
        .option("driver", dbSettings.driver.value)
        .option("dbtable", tableDef.tableName)
        .load())

  def uploadCsv(path: String)(implicit spark: SparkSession): F[Unit] = {
    val opts = FileFormat.Csv.defaultOptions ++ sparkOptions
    uploadToDB(TypedDataset.createUnsafe[A](spark.read.options(opts).csv(path)))
  }

  def uploadJson(path: String)(implicit spark: SparkSession): F[Unit] = {
    val opts = FileFormat.Json.defaultOptions ++ sparkOptions
    uploadToDB(TypedDataset.createUnsafe[A](spark.read.options(opts).json(path)))
  }

  def uploadParquet(path: String)(implicit spark: SparkSession): F[Unit] = {
    val opts = FileFormat.Parquet.defaultOptions ++ sparkOptions
    uploadToDB(TypedDataset.createUnsafe[A](spark.read.options(opts).parquet(path)))
  }

  def uploadTopic[K: TypedEncoder](topic: => KafkaTopic[F, K, A])(
    implicit spark: SparkSession): F[Unit] =
    Sparkafka
      .datasetFromKafka(topic)
      .map(ds => ds.select(ds('value)).deserialized.flatMap(x => x))
      .flatMap(uploadToDB)

  def uploadToDB(data: TypedDataset[A]): F[Unit] =
    Sync[F].delay(
      data.write
        .mode(saveMode)
        .format("jdbc")
        .option("url", dbSettings.connStr.value)
        .option("driver", dbSettings.driver.value)
        .option("dbtable", tableDef.tableName)
        .save())

  // doobie
  val source: Stream[F, A] = {
    for {
      xa <- dbSettings.transactorStream[F]
      dt: Stream[ConnectionIO, A] = (fr"select * from" ++ Fragment.const(tableDef.tableName))
        .query[A]
        .stream
      rst <- xa.transP.apply(dt)
    } yield rst
  }
}
