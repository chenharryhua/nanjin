package com.github.chenharryhua.nanjin.common

import cats.instances.int._
import enumeratum.values.{CatsOrderValueEnum, IntEnum, IntEnumEntry}
import monocle.Prism
import monocle.macros.GenPrism

import scala.collection.immutable

sealed abstract class NJFileFormat(val value: Int, val format: String, val alias: String)
    extends IntEnumEntry with Serializable {
  final override def toString: String = format
}

object NJFileFormat extends CatsOrderValueEnum[Int, NJFileFormat] with IntEnum[NJFileFormat] {
  override val values: immutable.IndexedSeq[NJFileFormat] = findValues
//text
  case object Jackson extends NJFileFormat(1, "json", "jackson")
  case object Json extends NJFileFormat(2, "json", "circe")
  case object Text extends NJFileFormat(3, "txt", "plain")
  case object Csv extends NJFileFormat(4, "csv", "kantan")
//binary
  case object Parquet extends NJFileFormat(10, "parquet", "apache")
  case object Avro extends NJFileFormat(11, "avro", "data")
  case object BinaryAvro extends NJFileFormat(12, "avro", "binary")
  case object JavaObject extends NJFileFormat(13, "obj", "java")

  type Jackson    = Jackson.type
  type Json       = Json.type
  type Text       = Text.type
  type Csv        = Csv.type
  type Parquet    = Parquet.type
  type Avro       = Avro.type
  type BinaryAvro = BinaryAvro.type
  type JavaObject = JavaObject.type

  implicit val prismJackson: Prism[NJFileFormat, Jackson] =
    GenPrism[NJFileFormat, Jackson]

  implicit val prismParquet: Prism[NJFileFormat, Parquet] =
    GenPrism[NJFileFormat, Parquet]

  implicit val prismAvro: Prism[NJFileFormat, Avro] =
    GenPrism[NJFileFormat, Avro]

}

trait UpdateParams[A, B] {
  def withParamUpdate(f: A => A): B
}
