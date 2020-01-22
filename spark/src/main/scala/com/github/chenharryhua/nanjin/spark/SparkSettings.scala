package com.github.chenharryhua.nanjin.spark

import cats.effect.{Resource, Sync}
import fs2.Stream
import monocle.macros.Lenses
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

@Lenses final case class SparkSettings(conf: SparkConf, logLevel: String) {

  def withKms(kmsKey: String): SparkSettings =
    withConfigUpdate(
      _.set("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
        .set("spark.hadoop.fs.s3a.server-side-encryption.key", kmsKey))

  def withMaster(master: String): SparkSettings =
    withConfigUpdate(_.set("spark.master", master))

  def withLogLevel(logLevel: String): SparkSettings =
    SparkSettings.logLevel.set(logLevel)(this)

  def withConfigUpdate(f: SparkConf => SparkConf): SparkSettings =
    SparkSettings.conf.modify(f)(this)

  def session: SparkSession = {
    val spk = SparkSession.builder().config(conf).getOrCreate()
    spk.sparkContext.setLogLevel(logLevel)
    spk
  }

  def sessionResource[F[_]: Sync]: Resource[F, SparkSession] =
    Resource.make(Sync[F].delay(session))(spk => Sync[F].delay(spk.close()))

  def sessionStream[F[_]: Sync]: Stream[F, SparkSession] =
    Stream.resource(sessionResource)
}

object SparkSettings {

  val default: SparkSettings =
    SparkSettings(new SparkConf, "warn").withConfigUpdate(
      _.set("spark.master", "local[*]")
        .set("spark.ui.enabled", "true")
        .set("spark.network.timeout", "800")
        .set("spark.debug.maxToStringFields", "1000")
        .set(
          "spark.hadoop.fs.s3a.aws.credentials.provider",
          "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .set("spark.hadoop.fs.s3a.connection.maximum", "100")
        .set("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential")
        .set("spark.streaming.kafka.consumer.poll.ms", "180000")
        .set("spark.streaming.kafka.allowNonConsecutiveOffsets", "true"))
}
