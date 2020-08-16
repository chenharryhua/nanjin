package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.database.{DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.spark.saver.RddFileSaver
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession

final class TableDef[A] private (val tableName: TableName)(implicit
  val typedEncoder: TypedEncoder[A]) {

  def in(dbSettings: DatabaseSettings)(implicit sparkSession: SparkSession): SparkTable[A] =
    new SparkTable[A](this, dbSettings, STConfig.defaultConfig)
}

object TableDef {

  def apply[A: TypedEncoder](tableName: TableName): TableDef[A] =
    new TableDef[A](tableName)
}

final class SparkTable[A](tableDef: TableDef[A], dbSettings: DatabaseSettings, cfg: STConfig)(
  implicit sparkSession: SparkSession)
    extends UpdateParams[STConfig, SparkTable[A]] {
  import tableDef.typedEncoder

  val params: STParams = cfg.evalConfig

  override def withParamUpdate(f: STConfig => STConfig): SparkTable[A] =
    new SparkTable[A](tableDef, dbSettings, f(cfg))

  def fromDB: TypedDataset[A] =
    sd.fromDB(dbSettings.connStr, dbSettings.driver, tableDef.tableName)

  def save[F[_]]: RddFileSaver[F, A] = new RddFileSaver[F, A](fromDB.dataset.rdd)

  def fromDisk(path: String): TypedDataset[A] =
    sd.fromDisk(params.fileFormat, path)

  def fromDisk: TypedDataset[A] =
    fromDisk(params.pathBuilder(tableDef.tableName, params.fileFormat))

  def upload(dataset: TypedDataset[A]): Unit =
    sd.upload(dataset, params.dbSaveMode, dbSettings.connStr, dbSettings.driver, tableDef.tableName)
}
