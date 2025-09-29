package com.github.chenharryhua.nanjin.terminals

import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import shapeless.{:+:, CNil}

import scala.collection.immutable

sealed abstract class FileFormat(val value: Int, val format: String, val alias: String)
    extends EnumEntry with Product {

  final def suffix: String = s"$alias.$format"

  final override def entryName: String = suffix
}

object FileFormat extends Enum[FileFormat] with CatsEnum[FileFormat] with CirceEnum[FileFormat] {
  override val values: immutable.IndexedSeq[FileFormat] = findValues

  case object Unknown extends FileFormat(-1, "unknown", "unknown")

  // text
  case object Jackson extends FileFormat(1, "json", "jackson")
  case object Circe extends FileFormat(2, "json", "circe")
  case object Text extends FileFormat(3, "txt", "plain")
  case object Kantan extends FileFormat(4, "csv", "kantan")

  // binary
  case object Parquet extends FileFormat(11, "parquet", "apache")
  case object Avro extends FileFormat(12, "avro", "data")
  case object BinaryAvro extends FileFormat(13, "avro", "binary")
  case object JavaObject extends FileFormat(14, "obj", "java")
  case object ProtoBuf extends FileFormat(15, "pb", "google")

  // types
  type Jackson = Jackson.type
  type Circe = Circe.type
  type Text = Text.type
  type Kantan = Kantan.type
  type Parquet = Parquet.type
  type Avro = Avro.type
  type BinaryAvro = BinaryAvro.type
  type JavaObject = JavaObject.type
  type ProtoBuf = ProtoBuf.type

  type JsonFamily = Jackson :+: Circe :+: CNil

  type TextFamily = Jackson :+: Circe :+: Text :+: Kantan :+: CNil

  type BinaryFamily =
    Parquet :+: Avro :+: BinaryAvro :+: JavaObject :+: ProtoBuf :+: CNil

  type AvroFamily = Jackson :+: Parquet :+: Avro :+: BinaryAvro :+: CNil

}
