package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.database.{ConnectionString, DriverString, TableName}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

private[database] object sd {

  def unload[A: TypedEncoder](
    connStr: ConnectionString,
    driver: DriverString,
    tableName: TableName,
    query: Option[String])(implicit sparkSession: SparkSession): TypedDataset[A] = {
    val mandatory = Map("url" -> connStr.value, "driver" -> driver.value)
    //https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    val sparkOptions: Map[String, String] =
      mandatory + query.fold("dbtable" -> tableName.value)(q => "query" -> q)
    TypedDataset.createUnsafe[A](sparkSession.read.format("jdbc").options(sparkOptions).load())
  }

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
