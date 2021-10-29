package com.github.chenharryhua.nanjin.common

import enumeratum.values.{CatsOrderValueEnum, IntCirceEnum, IntEnum, IntEnumEntry}
import monocle.Prism
import monocle.generic.coproduct.coProductPrism
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

  // json family
  type JsonFamily = Jackson :+: Circe :+: SparkJson :+: CNil

  implicit val jsonPrimsJackson: Prism[JsonFamily, Jackson] =
    coProductPrism[JsonFamily, Jackson]

  implicit val jsonPrimsCirce: Prism[JsonFamily, Circe] =
    coProductPrism[JsonFamily, Circe]

  implicit val jsonPrimsSparkJson: Prism[JsonFamily, SparkJson] =
    coProductPrism[JsonFamily, SparkJson]

  // text family
  type TextFamily = Jackson :+: Circe :+: Text :+: Csv :+: SparkJson :+: CNil

  implicit val textPrismJackson: Prism[TextFamily, Jackson] =
    coProductPrism[TextFamily, Jackson]

  implicit val textPrismJson: Prism[TextFamily, Circe] =
    coProductPrism[TextFamily, Circe]

  implicit val textPrismText: Prism[TextFamily, Text] =
    coProductPrism[TextFamily, Text]

  implicit val textPrismCsv: Prism[TextFamily, Csv] =
    coProductPrism[TextFamily, Csv]

  implicit val textPrismSparkJson: Prism[TextFamily, SparkJson] =
    coProductPrism[TextFamily, SparkJson]

  // binary family
  type BinaryFamily =
    Parquet :+: Avro :+: BinaryAvro :+: JavaObject :+: ProtoBuf :+: CNil

  implicit val binPrismJavaObject: Prism[BinaryFamily, JavaObject] =
    coProductPrism[BinaryFamily, JavaObject]

  implicit val binPrismBinaryAvro: Prism[BinaryFamily, BinaryAvro] =
    coProductPrism[BinaryFamily, BinaryAvro]

  implicit val binPrismParquet: Prism[BinaryFamily, Parquet] =
    coProductPrism[BinaryFamily, Parquet]

  implicit val binPrismAvro: Prism[BinaryFamily, Avro] =
    coProductPrism[BinaryFamily, Avro]

  implicit val binPrismProtobuf: Prism[BinaryFamily, ProtoBuf] =
    coProductPrism[BinaryFamily, ProtoBuf]

  // avro family
  type AvroFamily = Jackson :+: Parquet :+: Avro :+: BinaryAvro :+: CNil

  implicit val avroPrismJackson: Prism[AvroFamily, Jackson] =
    coProductPrism[AvroFamily, Jackson]

  implicit val avroPrismBinaryAvro: Prism[AvroFamily, BinaryAvro] =
    coProductPrism[AvroFamily, BinaryAvro]

  implicit val avroPrismParquet: Prism[AvroFamily, Parquet] =
    coProductPrism[AvroFamily, Parquet]

  implicit val avroPrismAvro: Prism[AvroFamily, Avro] =
    coProductPrism[AvroFamily, Avro]

}
