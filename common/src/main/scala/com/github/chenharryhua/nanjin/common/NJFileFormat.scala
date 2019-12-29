package com.github.chenharryhua.nanjin.common

sealed abstract class NJFileFormat {
  val props: Map[String, String]
}

object NJFileFormat {
  final case class Csv(props: Map[String, String]) extends NJFileFormat
  final case class Json(props: Map[String, String]) extends NJFileFormat
  final case class Parquet(props: Map[String, String]) extends NJFileFormat
  final case class Avro(props: Map[String, String]) extends NJFileFormat
}
