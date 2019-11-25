package com.github.chenharryhua.nanjin.hadoop

import monocle.function.At.at
import monocle.macros.Lenses
import java.{util => ju}

import org.apache.hadoop.conf.Configuration

@Lenses final case class HadoopSettings(config: Map[String, String]) {

  def set(key: String, value: String): HadoopSettings =
    HadoopSettings.config.composeLens(at(key)).set(Some(value))(this)

  def toSpark: Map[String, String] = config.map { case (k, v) => (s"spark.hadoop.$k" -> v) }
}

object HadoopSettings {

  val default: HadoopSettings = HadoopSettings(
    Map(
      "fs.s3.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
      "fs.s3a.aws.credentials.provider" -> "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
      "fs.s3a.connection.maximum" -> "100",
      "fs.s3a.experimental.input.fadvise" -> "sequential"
    )
  )
}
