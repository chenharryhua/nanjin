package com.github.chenharryhua.nanjin.spark

import cats.effect.kernel.{Resource, Sync}
import com.github.chenharryhua.nanjin.common.{NJLogLevel, UpdateConfig}
import fs2.Stream
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.time.ZoneId

/** [[http://spark.apache.org/]]
  */

final case class SparkSettings private (conf: SparkConf, logLevel: NJLogLevel)
    extends UpdateConfig[SparkConf, SparkSettings] {

  def withAppName(appName: String): SparkSettings = updateConfig(_.setAppName(appName))
  def withMaster(master: String): SparkSettings   = updateConfig(_.setMaster(master))
  def withHome(home: String): SparkSettings       = updateConfig(_.setSparkHome(home))

  def withKms(kmsKey: String): SparkSettings = {
    val kms = if (kmsKey.startsWith("alias/")) kmsKey else s"alias/$kmsKey"
    updateConfig(
      _.set("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
        .set("spark.hadoop.fs.s3a.server-side-encryption.key", kms))
  }

  def withLogLevel(njLogLevel: NJLogLevel): SparkSettings = copy(logLevel = njLogLevel)

  def withUI: SparkSettings    = updateConfig(_.set("spark.ui.enabled", "true"))
  def withoutUI: SparkSettings = updateConfig(_.set("spark.ui.enabled", "false"))

  def withZoneId(zoneId: ZoneId): SparkSettings = updateConfig(_.set("spark.sql.session.timeZone", zoneId.getId))

  def updateConfig(f: SparkConf => SparkConf): SparkSettings = copy(conf = f(conf))

  def unsafeSession: SparkSession = {
    val spk: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    spk.sparkContext.setLogLevel(logLevel.entryName)
    spk
  }

  def sessionResource[F[_]: Sync]: Resource[F, SparkSession] =
    Resource.make(Sync[F].blocking(unsafeSession))(spk => Sync[F].blocking(spk.close()))

  def sessionStream[F[_]: Sync]: Stream[F, SparkSession] = Stream.resource(sessionResource)
}

object SparkSettings {

  def apply(zoneId: ZoneId): SparkSettings =
    SparkSettings(new SparkConf, NJLogLevel.WARN)
      .withAppName("nj-spark")
      .withMaster("local[*]")
      .withUI
      .withZoneId(zoneId)
      .updateConfig(
        _.set("spark.network.timeout", "800")
          .set("spark.debug.maxToStringFields", "1000")
          .set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
          .set("spark.hadoop.fs.s3a.connection.maximum", "100")
          .set("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential")
          .set("spark.hadoop.fs.s3a.committer.name", "directory")
          .set("spark.hadoop.fs.s3a.committer.staging.unique-filenames", "false")
          .set("spark.streaming.kafka.consumer.poll.ms", "180000")
          .set("spark.streaming.kafka.allowNonConsecutiveOffsets", "true")
          .set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true"))
}
