package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.common.database.TableName
import com.zaxxer.hikari.HikariConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

private[spark] object sd {

  def unloadDF(
    cfg: HikariConfig,
    tableName: TableName,
    query: Option[String],
    sparkSession: SparkSession): DataFrame = {
    val mandatory = Map(
      "url" -> cfg.getJdbcUrl,
      "driver" -> cfg.getDriverClassName,
      "user" -> cfg.getUsername,
      "password" -> cfg.getPassword)
    // https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    val sparkOptions: Map[String, String] = mandatory + query.fold("dbtable" -> tableName.value)(q => "query" -> q)
    sparkSession.read.format("jdbc").options(sparkOptions).load()
  }
}
