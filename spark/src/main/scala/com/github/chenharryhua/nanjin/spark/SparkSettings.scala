package com.github.chenharryhua.nanjin.spark

import cats.effect.kernel.{Resource, Sync}
import com.github.chenharryhua.nanjin.common.{NJLogLevel, UpdateConfig}
import fs2.Stream
import monocle.macros.Lenses
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** [[http://spark.apache.org/]]
  */

@Lenses final case class SparkSettings(conf: SparkConf, logLevel: NJLogLevel)
    extends UpdateConfig[SparkConf, SparkSettings] {

  def withAppName(appName: String): SparkSettings =
    updateConfig(_.set("spark.app.name", appName))

  def withKms(kmsKey: String): SparkSettings = {
    val kms = if (kmsKey.startsWith("alias/")) kmsKey else s"alias/$kmsKey"
    updateConfig(
      _.set("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
        .set("spark.hadoop.fs.s3a.server-side-encryption.key", kms))
  }

  def withMaster(master: String): SparkSettings =
    updateConfig(_.set("spark.master", master))

  def withLogLevel(logLevel: NJLogLevel): SparkSettings =
    SparkSettings.logLevel.set(logLevel)(this)

  def withUI: SparkSettings =
    updateConfig(_.set("spark.ui.enabled", "true"))

  def withoutUI: SparkSettings =
    updateConfig(_.set("spark.ui.enabled", "false"))

  def updateConfig(f: SparkConf => SparkConf): SparkSettings =
    SparkSettings.conf.modify(f)(this)

  def session: SparkSession = {
    val spk = SparkSession.builder().config(conf).getOrCreate()
    spk.sparkContext.setLogLevel(logLevel.entryName)
    spk
  }

  def sessionResource[F[_]: Sync]: Resource[F, SparkSession] =
    Resource.make(Sync[F].blocking(session))(spk => Sync[F].blocking(spk.close()))

  def sessionStream[F[_]: Sync]: Stream[F, SparkSession] =
    Stream.resource(sessionResource)
}

object SparkSettings {

  val default: SparkSettings =
    SparkSettings(new SparkConf, NJLogLevel.WARN)
      .withAppName("nj-spark")
      .withMaster("local[*]")
      .withUI
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
