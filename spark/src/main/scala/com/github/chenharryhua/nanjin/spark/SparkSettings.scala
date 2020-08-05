package com.github.chenharryhua.nanjin.spark

import cats.effect.{Resource, Sync}
import com.github.chenharryhua.nanjin.common.NJLogLevel
import fs2.Stream
import monocle.macros.Lenses
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}

@Lenses final case class SparkSettings(conf: SparkConf, logLevel: NJLogLevel) {

  def withAppName(appName: String): SparkSettings =
    withConfigUpdate(_.set("spark.app.name", appName))

  def withKms(kmsKey: String): SparkSettings = {
    val kms = if (kmsKey.startsWith("alias/")) kmsKey else s"alias/$kmsKey"
    withConfigUpdate(
      _.set("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
        .set("spark.hadoop.fs.s3a.server-side-encryption.key", kms))
  }

  def withMaster(master: String): SparkSettings =
    withConfigUpdate(_.set("spark.master", master))

  def withLogLevel(logLevel: NJLogLevel): SparkSettings =
    SparkSettings.logLevel.set(logLevel)(this)

  def withUI: SparkSettings =
    withConfigUpdate(_.set("spark.ui.enabled", "true"))

  def withoutUI: SparkSettings =
    withConfigUpdate(_.set("spark.ui.enabled", "false"))

  def withConfigUpdate(f: SparkConf => SparkConf): SparkSettings =
    SparkSettings.conf.modify(f)(this)

  def session: SparkSession = {
    val spk = SparkSession.builder().config(conf).getOrCreate()
    spk.sparkContext.setLogLevel(logLevel.entryName)
    spk
  }

  def sessionResource[F[_]: Sync]: Resource[F, SparkSession] =
    Resource.make(Sync[F].delay(session))(spk => Sync[F].delay(spk.close()))

  def sessionStream[F[_]: Sync]: Stream[F, SparkSession] =
    Stream.resource(sessionResource)

  def streamingContext(batchDuration: Duration): StreamingContext =
    new StreamingContext(conf, batchDuration)

}

object SparkSettings {

  val default: SparkSettings =
    SparkSettings(new SparkConf, NJLogLevel.WARN)
      .withAppName("nj-spark")
      .withMaster("local[*]")
      .withUI
      .withConfigUpdate(
        _.set("spark.network.timeout", "800")
          .set("spark.debug.maxToStringFields", "1000")
          .set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
          .set("spark.hadoop.fs.s3a.connection.maximum", "100")
          .set("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential")
          .set("spark.hadoop.fs.s3a.committer.name", "directory")
          .set("spark.hadoop.fs.s3a.committer.staging.unique-filenames", "false")
          .set("spark.streaming.kafka.consumer.poll.ms", "180000")
          .set("spark.streaming.kafka.allowNonConsecutiveOffsets", "true"))
}
