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
    TypedDataset.createUnsafe[A](
      sparkSession.read
        .format("jdbc")
        .option("url", dbSettings.connStr.value)
        .option("driver", dbSettings.driver.value)
        .option("dbtable", tableDef.tableName.value)
        .load())

  def save(): Unit =
    fromDB.write
      .mode(params.fileSaveMode)
      .format(params.fileFormat.format)
      .save(params.getPath(tableDef.tableName))

  def fromDisk: TypedDataset[A] =
    TypedDataset.createUnsafe[A](
      sparkSession.read.format(params.fileFormat.format).load(params.getPath(tableDef.tableName)))

  def upload(data: TypedDataset[A]): Unit =
    data.write
      .mode(params.dbSaveMode)
      .format("jdbc")
      .option("url", dbSettings.connStr.value)
      .option("driver", dbSettings.driver.value)
      .option("dbtable", tableDef.tableName.value)
      .save()
}
