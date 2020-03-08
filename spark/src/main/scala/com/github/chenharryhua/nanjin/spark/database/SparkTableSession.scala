package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.database.{DatabaseSettings, TableName}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession

final class TableDef[A] private (val tableName: TableName)(
  implicit val typedEncoder: TypedEncoder[A]) {

  def in(dbSettings: DatabaseSettings)(implicit sparkSession: SparkSession): SparkTableSession[A] =
    new SparkTableSession[A](this, dbSettings, STConfig.defaultConfig)
}

object TableDef {

  def apply[A: TypedEncoder](tableName: TableName): TableDef[A] =
    new TableDef[A](tableName)
}

final class SparkTableSession[A](tableDef: TableDef[A], dbSettings: DatabaseSettings, cfg: STConfig)(
  implicit sparkSession: SparkSession)
    extends UpdateParams[STConfig, SparkTableSession[A]] {
  import tableDef.typedEncoder

  val params: STParams = STConfigF.evalConfig(cfg)

  override def withParamUpdate(f: STConfig => STConfig): SparkTableSession[A] =
    new SparkTableSession[A](tableDef, dbSettings, f(cfg))

  def fromDB: TypedDataset[A] =
    sd.fromDB(dbSettings.connStr, dbSettings.driver, tableDef.tableName)

  def save(path: String): Unit =
    sd.save(fromDB, params.fileSaveMode, params.fileFormat, path)

  def save(): Unit =
    save(params.pathBuilder(TablePathBuild(tableDef.tableName, params.fileFormat)))

  def fromDisk(path: String): TypedDataset[A] =
    sd.fromDisk(params.fileFormat, path)

  def fromDisk(): TypedDataset[A] =
    fromDisk(params.pathBuilder(TablePathBuild(tableDef.tableName, params.fileFormat)))

  def upload(dataset: TypedDataset[A]): Unit =
    sd.upload(dataset, params.dbSaveMode, dbSettings.connStr, dbSettings.driver, tableDef.tableName)
}
