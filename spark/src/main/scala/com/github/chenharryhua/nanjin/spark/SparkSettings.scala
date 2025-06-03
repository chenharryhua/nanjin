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
    new SparkSettings(new SparkConf)
      .withAppName("nj-spark")
      .withMaster("local[*]")
      .withUI
      .withZoneId(zoneId)
      .updateConfig(
        _.set("spark.network.timeout", "800")
          .set("spark.debug.maxToStringFields", "1000")
          .set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
          .set("spark.hadoop.fs.s3a.connection.maximum", "100")
          .set("spark.hadoop.fs.s3a.connection.establish.timeout", "15000")
          .set("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential")
          .set("spark.hadoop.fs.s3a.committer.name", "directory")
          .set("spark.hadoop.fs.s3a.committer.staging.unique-filenames", "false")
          .set("spark.streaming.kafka.consumer.poll.ms", "180000")
          .set("spark.streaming.kafka.allowNonConsecutiveOffsets", "true")
          .set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true"))
}
