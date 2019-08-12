package com.github.chenharryhua.nanjin.sparkafka

import cats.Show
import monocle.macros.Lenses
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

@Lenses case class SparkSettings(conf: SparkConf = new SparkConf) {

  def kms(kmsKey: String): SparkSettings =
    SparkSettings.conf.set(
      conf
        .set("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
        .set("spark.hadoop.fs.s3a.server-side-encryption.key", kmsKey))(this)

  def set(key: String, value: String): SparkSettings =
    SparkSettings.conf.set(conf.set(key, value))(this)

  def updateConf(f: SparkConf => SparkConf): SparkSettings =
    SparkSettings.conf.set(f(conf))(this)

  def session: SparkSession =
    SparkSession.builder().config(conf).getOrCreate()

  def show: String = conf.toDebugString
}

object SparkSettings {
  implicit val showSparkSettings: Show[SparkSettings] = _.show

  val default: SparkSettings = SparkSettings()
    .set("spark.master", "local[*]")
    .set("spark.ui.enabled", "true")
    .set("spark.debug.maxToStringFields", "1000")
    .set(
      "spark.hadoop.fs.s3a.aws.credentials.provider",
      "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .set("spark.hadoop.fs.s3a.connection.maximum", "100")
    .set("spark.network.timeout", "800")
    .set("spark.streaming.kafka.consumer.poll.ms", "180000")
    .set("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential")
}
