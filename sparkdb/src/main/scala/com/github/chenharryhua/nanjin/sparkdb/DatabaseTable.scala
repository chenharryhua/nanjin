package com.github.chenharryhua.nanjin.sparkdb

import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.{SaveMode, SparkSession}

final case class TableDef[A: TypedEncoder](schema: String, table: String) {
  val tableName: String = s"$schema.$table"

  def in(dbSettings: DatabaseSettings): DatabaseTable[A] =
    DatabaseTable[A](this, dbSettings)
}

final case class DatabaseTable[A](tableDef: TableDef[A], dbSettings: DatabaseSettings) {

  def dataset(implicit spark: SparkSession, ev: TypedEncoder[A]): TypedDataset[A] =
    TypedDataset.createUnsafe[A](
      spark.read
        .format("jdbc")
        .option("url", dbSettings.connStr.value)
        .option("driver", dbSettings.driver.value)
        .option("dbtable", tableDef.tableName)
        .load())

  private def updateDB(data: TypedDataset[A], saveMode: SaveMode): Unit =
    data.write
      .mode(saveMode)
      .format("jdbc")
      .option("url", dbSettings.connStr.value)
      .option("driver", dbSettings.driver.value)
      .option("dbtable", tableDef.tableName)
      .save()

  def appendDB(data: TypedDataset[A], db: DatabaseSettings): Unit =
    updateDB(data, SaveMode.Append)

  def overwriteDB(data: TypedDataset[A], db: DatabaseSettings): Unit =
    updateDB(data, SaveMode.Overwrite)

}
