package com.github.chenharryhua.nanjin.sparkafka

import cats.Show
import monocle.macros.Lenses
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

@Lenses case class SparkSettings(props: Map[String, String]) { outer =>

  def kms(kmsValue: String): SparkSettings =
    copy(
      props = props ++ Map(
        "spark.hadoop.fs.s3a.server-side-encryption-algorithm" -> "SSE-KMS",
        "spark.hadoop.fs.s3a.server-side-encryption.key" -> kmsValue))

  protected def conf: SparkConf = new SparkConf().setAll(props)

  def updateConf(f: SparkConf => SparkConf): SparkSettings = new SparkSettings(props) {
    override protected def conf: SparkConf = f(outer.conf)
  }

  def session: SparkSession =
    SparkSession.builder().config(conf).getOrCreate()

  def show: String = conf.toDebugString
}

object SparkSettings {
  implicit val showSparkSettings: Show[SparkSettings] = _.show

  val default: SparkSettings = SparkSettings(
    Map(
      "spark.master" -> "local[*]",
      "spark.app.name" -> "nanjin",
      "spark.ui.enabled" -> "true",
      "spark.debug.maxToStringFields" -> "1000",
      "spark.hadoop.fs.s3a.aws.credentials.provider" -> "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
      "spark.hadoop.fs.s3a.connection.maximum" -> "100",
      "spark.network.timeout" -> "800",
      "spark.streaming.kafka.consumer.poll.ms" -> "180000",
      "spark.hadoop.fs.s3a.experimental.input.fadvise" -> "sequential"
    )
  )
}
