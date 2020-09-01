package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.database.{ConnectionString, DriverString, TableName}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.{DataFrame, SparkSession}

private[spark] object sd {

  def unloadDF(
    connStr: ConnectionString,
    driver: DriverString,
    tableName: TableName,
    query: Option[String])(implicit sparkSession: SparkSession): DataFrame = {
    val mandatory = Map("url" -> connStr.value, "driver" -> driver.value)
    //https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    val sparkOptions: Map[String, String] =
      mandatory + query.fold("dbtable" -> tableName.value)(q => "query" -> q)
    sparkSession.read.format("jdbc").options(sparkOptions).load()
  }

  def unloadDS[A](
    connStr: ConnectionString,
    driver: DriverString,
    tableName: TableName,
    query: Option[String])(implicit
    te: TypedEncoder[A],
    sparkSession: SparkSession): TypedDataset[A] =
    TypedDataset.createUnsafe[A](unloadDF(connStr, driver, tableName, query))
}
