package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.database.{DatabaseSettings, TableName}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession

final case class TableDef[A] private (tableName: TableName)(
  implicit val typedEncoder: TypedEncoder[A]) {

  def in(dbSettings: DatabaseSettings)(implicit sparkSession: SparkSession): SparkTableSession[A] =
    new SparkTableSession[A](this, dbSettings, SparkTableParams.default)
}

object TableDef {

  def apply[A: TypedEncoder](tableName: String): TableDef[A] = TableDef[A](TableName(tableName))
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
    st.fromDB(dbSettings.connStr, dbSettings.driver, tableDef.tableName)

  def save(): Unit =
    st.save(fromDB, params.fileSaveMode, params.fileFormat, params.getPath(tableDef.tableName))

  def fromDisk: TypedDataset[A] =
    st.fromDisk(params.fileFormat, params.getPath(tableDef.tableName))

  def upload(dataset: TypedDataset[A]): Unit =
    st.upload(dataset, params.dbSaveMode, dbSettings.connStr, dbSettings.driver, tableDef.tableName)
}
