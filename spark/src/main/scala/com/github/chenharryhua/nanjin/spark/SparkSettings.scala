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

  def withAwsS3: SparkSettings =
    updateConfig(
      _.config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
        // basic
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", value = true)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", value = true)
        .config("spark.hadoop.fs.s3a.connection.maximum", 200)
        .config("spark.hadoop.fs.s3a.threads.max", 200)
        .config("spark.hadoop.fs.s3a.fast.upload", value = true)
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
        .config("spark.hadoop.fs.s3a.multipart.size", "64M")
        .config("spark.hadoop.fs.s3a.block.size", "64M")
        .config("spark.hadoop.fs.s3a.multipart.threshold", "64M")
        .config("spark.hadoop.fs.s3a.readahead.range", "64K")
        // committer
        .config(
          "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
          "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2)
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", value = true)
        .config("spark.hadoop.fs.s3a.committer.name", "directory")
        .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
        .config("spark.hadoop.fs.s3a.committer.staging.unique-filenames", value = true)
        .config("spark.hadoop.fs.s3a.committer.cleanup.parallel", value = true)
        // retry and timeout
        .config("spark.hadoop.fs.s3a.retry.limit", 20)
        .config("spark.hadoop.fs.s3a.retry.interval", "500ms")
        .config("spark.hadoop.fs.s3a.attempts.maximum", 20)
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", 5000)
        .config("spark.hadoop.fs.s3a.connection.timeout", 20000)
    )

  lazy val sparkSession: SparkSession = build(SparkSession.builder()).getOrCreate()
}

object SparkSettings {

  def apply(zoneId: ZoneId): SparkSettings =
    new SparkSettings(identity)
      .withAppName("nj-spark")
      .withMaster("local[*]")
      .updateConfig(
        _.config("spark.cleaner.referenceTracking.cleanCheckpoints", value = true)
          .config("spark.ui.enabled", "true")
          .config("spark.sql.session.timeZone", zoneId.getId))
}
