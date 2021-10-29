package com.github.chenharryhua.nanjin.common

import enumeratum.values.{CatsOrderValueEnum, IntCirceEnum, IntEnum, IntEnumEntry}
import shapeless.{:+:, CNil}

import scala.collection.immutable

sealed abstract class NJFileFormat(val value: Int, val format: String, val alias: String)
    extends IntEnumEntry with Serializable {
  final override def toString: String = format
  final def suffix: String            = s".$alias.$format"
}

object NJFileFormat
    extends CatsOrderValueEnum[Int, NJFileFormat] with IntEnum[NJFileFormat] with IntCirceEnum[NJFileFormat] {
  override val values: immutable.IndexedSeq[NJFileFormat] = findValues

  case object Unknown extends NJFileFormat(-1, "unknown", "unknown")

  // text
  case object Jackson extends NJFileFormat(1, "json", "jackson")
  case object Circe extends NJFileFormat(2, "json", "circe")
  case object Text extends NJFileFormat(3, "txt", "plain")
  case object Csv extends NJFileFormat(4, "csv", "kantan")
  case object SparkJson extends NJFileFormat(5, "json", "spark")

  // binary
  case object Parquet extends NJFileFormat(11, "parquet", "apache")
  case object Avro extends NJFileFormat(12, "avro", "data")
  case object BinaryAvro extends NJFileFormat(13, "avro", "binary")
  case object JavaObject extends NJFileFormat(14, "obj", "java")
  case object ProtoBuf extends NJFileFormat(15, "pb", "google")

  // types
  type Jackson    = Jackson.type
  type Circe      = Circe.type
  type SparkJson  = SparkJson.type
  type Text       = Text.type
  type Csv        = Csv.type
  type Parquet    = Parquet.type
  type Avro       = Avro.type
  type BinaryAvro = BinaryAvro.type
  type JavaObject = JavaObject.type
  type ProtoBuf   = ProtoBuf.type

  type JsonFamily = Jackson :+: Circe :+: SparkJson :+: CNil

  type TextFamily = Jackson :+: Circe :+: Text :+: Csv :+: SparkJson :+: CNil

  type BinaryFamily =
    Parquet :+: Avro :+: BinaryAvro :+: JavaObject :+: ProtoBuf :+: CNil

  type AvroFamily = Jackson :+: Parquet :+: Avro :+: BinaryAvro :+: CNil

}
