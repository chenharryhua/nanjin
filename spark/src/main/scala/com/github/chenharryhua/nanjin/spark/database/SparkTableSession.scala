package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.database.{DatabaseSettings, TableName}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession

final case class TableDef[A] private (tableName: TableName)(
  implicit val typedEncoder: TypedEncoder[A]) {

  def in(dbSettings: DatabaseSettings)(implicit sparkSession: SparkSession): SparkTableSession[A] =
    SparkTableSession[A](this, dbSettings, SparkTableParams.default)
}

object TableDef {

  def apply[A: TypedEncoder](tableName: String): TableDef[A] = TableDef[A](TableName(tableName))
}

final case class SparkTableSession[A](
  tableDef: TableDef[A],
  dbSettings: DatabaseSettings,
  params: SparkTableParams)(implicit sparkSession: SparkSession)
    extends UpdateParams[SparkTableParams, SparkTableSession[A]] {
  import tableDef.typedEncoder

  def updateParams(f: SparkTableParams => SparkTableParams): SparkTableSession[A] =
    copy(params = f(params))

  def datasetFromDB: TypedDataset[A] =
    TypedDataset.createUnsafe[A](
      sparkSession.read
        .format("jdbc")
        .option("url", dbSettings.connStr.value)
        .option("driver", dbSettings.driver.value)
        .option("dbtable", tableDef.tableName.value)
        .load())

  def save(): Unit =
    datasetFromDB.write
      .mode(params.fileSaveMode)
      .format(params.fileFormat.format)
      .save(params.pathBuilder(tableDef.tableName))

  def load: TypedDataset[A] =
    TypedDataset.createUnsafe[A](
      sparkSession.read
        .format(params.fileFormat.format)
        .load(params.pathBuilder(tableDef.tableName)))

  def dbUpload(data: TypedDataset[A]): Unit =
    data.write
      .mode(params.dbSaveMode)
      .format("jdbc")
      .option("url", dbSettings.connStr.value)
      .option("driver", dbSettings.driver.value)
      .option("dbtable", tableDef.tableName.value)
      .save()
}
