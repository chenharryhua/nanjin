package com.github.chenharryhua.nanjin.sparkdb

import cats.effect.{Async, ContextShift, Resource, Sync}
import cats.implicits._
import doobie.hikari.HikariTransactor
import frameless.{TypedDataset, TypedEncoder}
import io.getquill.codegen.jdbc.SimpleJdbcCodegen
import org.apache.spark.sql.{SaveMode, SparkSession}

final case class TableDef[A](schema: String, table: String)(
  implicit val typedEncoder: TypedEncoder[A]) {
  val tableName: String = s"$schema.$table"

  def in(dbSettings: DatabaseSettings): DatabaseTable[A] =
    DatabaseTable[A](this, dbSettings)
}

final case class DatabaseTable[A](tableDef: TableDef[A], dbSettings: DatabaseSettings) {
  import tableDef.typedEncoder

  private def transactor[F[_]: ContextShift: Async]: Resource[F, HikariTransactor[F]] =
    dbSettings.transactor[F]

  def dataset(implicit spark: SparkSession): TypedDataset[A] =
    TypedDataset.createUnsafe[A](
      spark.read
        .format("jdbc")
        .option("url", dbSettings.connStr.value)
        .option("driver", dbSettings.driver.value)
        .option("dbtable", tableDef.tableName)
        .load())

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

  def genCaseClass[F[_]: ContextShift: Async]: F[Unit] = transactor.use {
    _.configure { hikari =>
      Sync[F]
        .delay(new SimpleJdbcCodegen(() => hikari.getConnection, ""))
        .map(_.writeStrings.toList.mkString("\n"))
        .map(println)
    }
  }
}
