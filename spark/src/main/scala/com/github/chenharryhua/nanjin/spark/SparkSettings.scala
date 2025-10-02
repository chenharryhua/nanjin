package com.github.chenharryhua.nanjin.spark

import cats.Endo
import com.github.chenharryhua.nanjin.common.UpdateConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.time.ZoneId

/** [[http://spark.apache.org/]]
  */

final class SparkSettings private (sparkConf: SparkConf) extends UpdateConfig[SparkConf, SparkSettings] {
  def updateConfig(f: Endo[SparkConf]): SparkSettings = new SparkSettings(sparkConf = f(sparkConf))

  def withAppName(appName: String): SparkSettings = updateConfig(_.setAppName(appName))
  def withMaster(master: String): SparkSettings = updateConfig(_.setMaster(master))
  def withHome(home: String): SparkSettings = updateConfig(_.setSparkHome(home))

  def withKms(kmsKey: String): SparkSettings = {
    val kms = if (kmsKey.startsWith("alias/")) kmsKey else s"alias/$kmsKey"
    updateConfig(
      _.set("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
        .set("spark.hadoop.fs.s3a.server-side-encryption.key", kms))
  }

  def withUI: SparkSettings = updateConfig(_.set("spark.ui.enabled", "true"))
  def withoutUI: SparkSettings = updateConfig(_.set("spark.ui.enabled", "false"))

  def withZoneId(zoneId: ZoneId): SparkSettings =
    updateConfig(_.set("spark.sql.session.timeZone", zoneId.getId))

  def sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
}

object SparkSettings {

  def apply(zoneId: ZoneId): SparkSettings =
    new SparkSettings(new SparkConf).withAppName("nj-spark").withMaster("local[*]").withUI.withZoneId(zoneId)
}
