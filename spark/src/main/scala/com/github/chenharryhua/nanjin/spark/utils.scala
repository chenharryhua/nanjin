package com.github.chenharryhua.nanjin.spark

import org.apache.spark.sql.SparkSession

import java.time.ZoneId

private[spark] object utils {
  final private val SPARK_ZONE_ID: String   = "spark.sql.session.timeZone"
  def sparkZoneId(ss: SparkSession): ZoneId = ZoneId.of(ss.conf.get(SPARK_ZONE_ID))
}
