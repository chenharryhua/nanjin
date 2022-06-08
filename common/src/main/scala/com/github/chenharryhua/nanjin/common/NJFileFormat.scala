package com.github.chenharryhua.nanjin.common

import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import shapeless.{:+:, CNil}

import scala.collection.immutable

sealed abstract class NJFileFormat(val value: Int, val format: String, val alias: String)
    extends EnumEntry with Product with Serializable {

  final def suffix: String = s"$alias.$format"

  final override def toString: String = suffix
}

object NJFileFormat extends Enum[NJFileFormat] with CatsEnum[NJFileFormat] with CirceEnum[NJFileFormat] {
  override val values: immutable.IndexedSeq[NJFileFormat] = findValues

  case object Unknown extends NJFileFormat(-1, "unknown", "unknown")

  // text
  case object Jackson extends NJFileFormat(1, "json", "jackson")
  case object Circe extends NJFileFormat(2, "json", "circe")
  case object Text extends NJFileFormat(3, "txt", "plain")
  case object Kantan extends NJFileFormat(4, "csv", "kantan")
  case object SparkJson extends NJFileFormat(5, "json", "spark")
  case object SparkCsv extends NJFileFormat(6, "csv", "spark")

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
  type Kantan     = Kantan.type
  type Parquet    = Parquet.type
  type Avro       = Avro.type
  type BinaryAvro = BinaryAvro.type
  type JavaObject = JavaObject.type
  type ProtoBuf   = ProtoBuf.type

  type JsonFamily = Jackson :+: Circe :+: SparkJson :+: CNil

  type TextFamily = Jackson :+: Circe :+: Text :+: Kantan :+: SparkJson :+: CNil

  type BinaryFamily =
    Parquet :+: Avro :+: BinaryAvro :+: JavaObject :+: ProtoBuf :+: CNil

  type AvroFamily = Jackson :+: Parquet :+: Avro :+: BinaryAvro :+: CNil

}
