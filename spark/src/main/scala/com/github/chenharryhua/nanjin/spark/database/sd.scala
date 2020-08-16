package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.database.{ConnectionString, DriverString, TableName}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

private[database] object sd {

  def unload[A: TypedEncoder](
    connStr: ConnectionString,
    driver: DriverString,
    tableName: TableName)(implicit sparkSession: SparkSession): TypedDataset[A] =
    TypedDataset.createUnsafe[A](
      sparkSession.read
        .format("jdbc")
        .option("url", connStr.value)
        .option("driver", driver.value)
        .option("dbtable", tableName.value)
        .load())

  def upload[A](
    dataset: Dataset[A],
    dbSaveMode: SaveMode,
    connStr: ConnectionString,
    driver: DriverString,
    tableName: TableName): Unit =
    dataset.write
      .mode(dbSaveMode)
      .format("jdbc")
      .option("url", connStr.value)
      .option("driver", driver.value)
      .option("dbtable", tableName.value)
      .save()
}
