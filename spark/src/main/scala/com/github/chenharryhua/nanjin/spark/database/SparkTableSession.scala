package com.github.chenharryhua.nanjin.spark.database

import cats.effect.{Concurrent, ContextShift, Sync}
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.database.DatabaseSettings
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.util.Read
import doobie.util.fragment.Fragment
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.sql.SparkSession

final case class TableDef[A](tableName: String)(
  implicit
  val typedEncoder: TypedEncoder[A],
  val doobieRead: Read[A]) {

  def in[F[_]: ContextShift: Concurrent](dbSettings: DatabaseSettings)(
    implicit sparkSession: SparkSession): SparkTableSession[F, A] =
    SparkTableSession[F, A](this, dbSettings, SparkTableParams.default)
}

final case class SparkTableSession[F[_]: ContextShift: Concurrent, A](
  tableDef: TableDef[A],
  dbSettings: DatabaseSettings,
  params: SparkTableParams)(implicit sparkSession: SparkSession)
    extends UpdateParams[SparkTableParams, SparkTableSession[F, A]] {
  import tableDef.{doobieRead, typedEncoder}

  private val path: String = params.rootPath + tableDef.tableName

  def updateParams(f: SparkTableParams => SparkTableParams): SparkTableSession[F, A] =
    copy(params = f(params))

  def datasetFromDB: TypedDataset[A] =
    TypedDataset.createUnsafe[A](
      sparkSession.read
        .format("jdbc")
        .option("url", dbSettings.connStr.value)
        .option("driver", dbSettings.driver.value)
        .option("dbtable", tableDef.tableName)
        .load())

  def saveToDisk: F[Unit] =
    Sync[F].delay(datasetFromDB.write.mode(params.fileSaveMode).parquet(path))

  def datasetFromDisk: TypedDataset[A] =
    TypedDataset.createUnsafe[A](sparkSession.read.parquet(path))

  def uploadToDB(data: TypedDataset[A]): F[Unit] =
    Sync[F].delay(
      data.write
        .mode(params.dbSaveMode)
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
