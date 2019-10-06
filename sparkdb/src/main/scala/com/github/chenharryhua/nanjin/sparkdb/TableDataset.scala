package com.github.chenharryhua.nanjin.sparkdb
import cats.effect.{Concurrent, ContextShift, Sync}
import doobie.Fragment
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.util.Read
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.sql.{SaveMode, SparkSession}

final case class TableDef[A](tableName: String)(
  implicit
  val typedEncoder: TypedEncoder[A],
  val doobieRead: Read[A]) {

  def in[F[_]: ContextShift: Concurrent](dbSettings: DatabaseSettings): TableDataset[F, A] =
    TableDataset[F, A](this, dbSettings)
}

final case class TableDataset[F[_]: ContextShift: Concurrent, A](
  tableDef: TableDef[A],
  dbSettings: DatabaseSettings) {
  import tableDef.{doobieRead, typedEncoder}

  // spark
  def datasetFromDB(implicit spark: SparkSession): TypedDataset[A] =
    TypedDataset.createUnsafe[A](
      spark.read
        .format("jdbc")
        .option("url", dbSettings.connStr.value)
        .option("driver", dbSettings.driver.value)
        .option("dbtable", tableDef.tableName)
        .load())

  def datasetFromCsv(path: String, options: Map[String, String] = Map("header" -> "true"))(
    implicit spark: SparkSession): TypedDataset[A] =
    TypedDataset.createUnsafe[A](spark.read.options(options).csv(path))

  def datasetFromJson(path: String, options: Map[String, String] = Map.empty)(
    implicit spark: SparkSession): TypedDataset[A] =
    TypedDataset.createUnsafe[A](spark.read.options(options).json(path))

  def datasetFromParquet(path: String)(implicit spark: SparkSession): TypedDataset[A] =
    TypedDataset.createUnsafe[A](spark.read.parquet(path))

  private def uploadToDB(data: TypedDataset[A], saveMode: SaveMode): F[Unit] =
    Sync[F].delay(
      data.write
        .mode(saveMode)
        .format("jdbc")
        .option("url", dbSettings.connStr.value)
        .option("driver", dbSettings.driver.value)
        .option("dbtable", tableDef.tableName)
        .save())

  def appendDB(data: TypedDataset[A]): F[Unit] = uploadToDB(data, SaveMode.Append)

  def overwriteDB(data: TypedDataset[A]): F[Unit] = uploadToDB(data, SaveMode.Overwrite)

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
