package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.database.{DatabaseSettings, TableName}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession

final class TableDef[A] private (val tableName: TableName)(
  implicit val typedEncoder: TypedEncoder[A]) {

  def in(dbSettings: DatabaseSettings)(implicit sparkSession: SparkSession): SparkTableSession[A] =
    new SparkTableSession[A](this, dbSettings, SparkTableParams.default)
}

object TableDef {

  def apply[A: TypedEncoder](tableName: String): TableDef[A] =
    new TableDef[A](TableName(tableName))
}

final class SparkTableSession[A](
  tableDef: TableDef[A],
  dbSettings: DatabaseSettings,
  params: SparkTableParams)(implicit sparkSession: SparkSession)
    extends UpdateParams[SparkTableParams, SparkTableSession[A]] {
  import tableDef.typedEncoder

  def withParamUpdate(f: SparkTableParams => SparkTableParams): SparkTableSession[A] =
    new SparkTableSession[A](tableDef, dbSettings, f(params))

  def fromDB: TypedDataset[A] =
    sd.fromDB(dbSettings.connStr, dbSettings.driver, tableDef.tableName)

  def save(path: String): Unit =
    sd.save(fromDB, params.fileSaveMode, params.fileFormat, path)

  def save(): Unit =
    save(params.getPath(tableDef.tableName))

  def fromDisk(path: String): TypedDataset[A] =
    sd.fromDisk(params.fileFormat, path)

  def fromDisk(): TypedDataset[A] =
    fromDisk(params.getPath(tableDef.tableName))

  def upload(dataset: TypedDataset[A]): Unit =
    sd.upload(dataset, params.dbSaveMode, dbSettings.connStr, dbSettings.driver, tableDef.tableName)
}
