package com.github.chenharryhua.nanjin.spark

import cats.Endo
import com.github.chenharryhua.nanjin.common.UpdateConfig
import org.apache.spark.sql.SparkSession

import java.time.ZoneId

/** [[http://spark.apache.org/]]
  */

final class SparkSettings private (build: Endo[SparkSession.Builder])
    extends UpdateConfig[SparkSession.Builder, SparkSettings] {
  def updateConfig(f: Endo[SparkSession.Builder]): SparkSettings = new SparkSettings(f.compose(build))

  def withAppName(appName: String): SparkSettings = updateConfig(_.appName(appName))
  def withMaster(master: String): SparkSettings = updateConfig(_.master(master))

  def withKms(kmsKey: String): SparkSettings = {
    val kms = if (kmsKey.startsWith("alias/")) kmsKey else s"alias/$kmsKey"
    updateConfig(
      _.config("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
        .config("spark.hadoop.fs.s3a.server-side-encryption.key", kms))
  }

  def withUI: SparkSettings = updateConfig(_.config("spark.ui.enabled", "true"))
  def withoutUI: SparkSettings = updateConfig(_.config("spark.ui.enabled", "false"))

  def withZoneId(zoneId: ZoneId): SparkSettings =
    updateConfig(_.config("spark.sql.session.timeZone", zoneId.getId))

  def sparkSession: SparkSession = build(SparkSession.builder()).getOrCreate()
}

object SparkSettings {

  def apply(zoneId: ZoneId): SparkSettings =
    new SparkSettings(identity).withAppName("nj-spark").withMaster("local[*]").withUI.withZoneId(zoneId)
}
