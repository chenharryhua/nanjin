package com.github.chenharryhua.nanjin.common

import cats.instances.int._
import enumeratum.values.{CatsOrderValueEnum, IntEnum, IntEnumEntry}
import monocle.Prism
import monocle.generic.coproduct.coProductPrism
import shapeless.{:+:, CNil}

import scala.collection.immutable

sealed abstract class NJFileFormat(val value: Int, val format: String, val alias: String)
    extends IntEnumEntry with Serializable {
  final override def toString: String = format
}

object NJFileFormat extends CatsOrderValueEnum[Int, NJFileFormat] with IntEnum[NJFileFormat] {
  override val values: immutable.IndexedSeq[NJFileFormat] = findValues

  //text
  case object Jackson extends NJFileFormat(1, "json", "jackson")
  case object CirceJson extends NJFileFormat(2, "json", "circe")
  case object Text extends NJFileFormat(3, "txt", "plain")
  case object Csv extends NJFileFormat(4, "csv", "kantan")

  //binary
  case object Parquet extends NJFileFormat(11, "parquet", "apache")
  case object Avro extends NJFileFormat(12, "avro", "data")
  case object BinaryAvro extends NJFileFormat(13, "avro", "binary")
  case object JavaObject extends NJFileFormat(14, "obj", "java")
  case object ProtoBuf extends NJFileFormat(15, "pb", "google")

  // multi
  case object MultiAvro extends NJFileFormat(21, "avro", "multi")
  case object MultiJackson extends NJFileFormat(22, "jackson", "multi")

  // types
  type Jackson      = Jackson.type
  type CirceJson    = CirceJson.type
  type Text         = Text.type
  type Csv          = Csv.type
  type Parquet      = Parquet.type
  type Avro         = Avro.type
  type BinaryAvro   = BinaryAvro.type
  type JavaObject   = JavaObject.type
  type ProtoBuf     = ProtoBuf.type
  type MultiAvro    = MultiAvro.type
  type MultiJackson = MultiJackson.type

  // json family
  type JsonFamily = Jackson :+: CirceJson :+: CNil

  implicit val jsonPrimsJackson: Prism[JsonFamily, Jackson] =
    coProductPrism[JsonFamily, Jackson]

  implicit val jsonPrimsJson: Prism[JsonFamily, CirceJson] =
    coProductPrism[JsonFamily, CirceJson]

  // text family
  type TextFamily = Jackson :+: CirceJson :+: Text :+: Csv :+: CNil

  implicit val textPrismJackson: Prism[TextFamily, Jackson] =
    coProductPrism[TextFamily, Jackson]

  implicit val textPrismJson: Prism[TextFamily, CirceJson] =
    coProductPrism[TextFamily, CirceJson]

  implicit val textPrismText: Prism[TextFamily, Text] =
    coProductPrism[TextFamily, Text]

  implicit val textPrismCsv: Prism[TextFamily, Csv] =
    coProductPrism[TextFamily, Csv]

  // binary family
  type BinaryFamily =
    Parquet :+: Avro :+: MultiAvro :+: BinaryAvro :+: JavaObject :+: ProtoBuf :+: CNil

  implicit val binPrismJavaObject: Prism[BinaryFamily, JavaObject] =
    coProductPrism[BinaryFamily, JavaObject]

  implicit val binPrismBinaryAvro: Prism[BinaryFamily, BinaryAvro] =
    coProductPrism[BinaryFamily, BinaryAvro]

  implicit val binPrismParquet: Prism[BinaryFamily, Parquet] =
    coProductPrism[BinaryFamily, Parquet]

  implicit val binPrismAvro: Prism[BinaryFamily, Avro] =
    coProductPrism[BinaryFamily, Avro]

  implicit val multiPrismAvro: Prism[BinaryFamily, MultiAvro] =
    coProductPrism[BinaryFamily, MultiAvro]

  implicit val binPrismProtobuf: Prism[BinaryFamily, ProtoBuf] =
    coProductPrism[BinaryFamily, ProtoBuf]

  // avro family
  type AvroFamily = Jackson :+: Parquet :+: Avro :+: MultiAvro :+: BinaryAvro :+: CNil

  implicit val avroPrismJackson: Prism[AvroFamily, Jackson] =
    coProductPrism[AvroFamily, Jackson]

  implicit val avroPrismBinaryAvro: Prism[AvroFamily, BinaryAvro] =
    coProductPrism[AvroFamily, BinaryAvro]

  implicit val avroPrismParquet: Prism[AvroFamily, Parquet] =
    coProductPrism[AvroFamily, Parquet]

  implicit val avroPrismMultiAvro: Prism[AvroFamily, MultiAvro] =
    coProductPrism[AvroFamily, MultiAvro]

  implicit val avroPrismAvro: Prism[AvroFamily, Avro] =
    coProductPrism[AvroFamily, Avro]

}

trait UpdateParams[A, B] {
  def withParamUpdate(f: A => A): B
}
