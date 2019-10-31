package com.github.chenharryhua.nanjin.spark

import monocle.macros.Lenses

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

final case class StorageRootPath(value: String) extends AnyVal

@Lenses final case class UploadRate(batchSize: Int, duration: FiniteDuration)

object UploadRate {
  val default: UploadRate = UploadRate(1000, 1.second)
}

private[spark] trait UpdateParams[A, B] {
  def updateParams(f: A => A): B
}

sealed trait FileFormat {
  def options: Map[String, String]
}

object FileFormat {

  case object Parquet extends FileFormat {
    override val options: Map[String, String] = Map.empty
  }

  case object Json extends FileFormat {
    override val options: Map[String, String] = Map.empty
  }

  case object Avro extends FileFormat {
    override val options: Map[String, String] = Map.empty
  }

  @Lenses final case class Csv(
    delimiter: Char,
    hasHeader: Boolean,
    quote: String,
    emptyValue: String,
    dateFormat: String,
    timestampFormat: String)
      extends FileFormat {

    override val options: Map[String, String] = Map(
      "header" -> hasHeader.toString,
      "delimiter" -> delimiter.toString,
      "quote" -> quote,
      "emtpyValue" -> emptyValue,
      "dateFormat" -> dateFormat,
      "timestampFormat" -> timestampFormat
    )
  }

  object Csv {

    val default: Csv = Csv(
      delimiter       = '|',
      hasHeader       = true,
      quote           = "\u0000",
      emptyValue      = "",
      dateFormat      = "YYYY-MM-DD",
      timestampFormat = "YYYY-MM-DD HH:mm:ss")
  }
}
