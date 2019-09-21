package com.github.chenharryhua.nanjin.sparkdb

import cats.effect.{Async, ContextShift, Resource, Sync}
import cats.implicits._
import doobie.hikari.HikariTransactor
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import io.getquill.codegen.jdbc.SimpleJdbcCodegen
import org.apache.spark.sql.{SaveMode, SparkSession}

final case class TableDef[A](schema: String, table: String)(
  implicit val typedEncoder: TypedEncoder[A]) {
  val tableName: String = s"$schema.$table"

  def in[F[_]: ContextShift: Async](dbSettings: DatabaseSettings): DatabaseTable[F, A] =
    DatabaseTable[F, A](this, dbSettings)
}

final case class DatabaseTable[F[_]: ContextShift: Async, A](
  tableDef: TableDef[A],
  dbSettings: DatabaseSettings) {
  import tableDef.typedEncoder

  private val transactor: Resource[F, HikariTransactor[F]] =
    dbSettings.transactor[F]

  def dataset(implicit spark: SparkSession): TypedDataset[A] =
    TypedDataset.createUnsafe[A](
      spark.read
        .format("jdbc")
        .option("url", dbSettings.connStr.value)
        .option("driver", dbSettings.driver.value)
        .option("dbtable", tableDef.tableName)
        .load())

  def dataStream =
    for { xa <- Stream.resource(transactor) } yield ()

  private def uploadToDB(data: TypedDataset[A], saveMode: SaveMode): Unit =
    data.write
      .mode(saveMode)
      .format("jdbc")
      .option("url", dbSettings.connStr.value)
      .option("driver", dbSettings.driver.value)
      .option("dbtable", tableDef.tableName)
      .save()

  def appendDB(data: TypedDataset[A]): Unit = uploadToDB(data, SaveMode.Append)

  def overwriteDB(data: TypedDataset[A]): Unit = uploadToDB(data, SaveMode.Overwrite)

  def genCaseClass: F[Unit] = transactor.use {
    _.configure { hikari =>
      Sync[F]
        .delay(new SimpleJdbcCodegen(() => hikari.getConnection, ""))
        .map(_.writeStrings.toList.mkString("\n"))
        .map(println)
    }
  }
}
