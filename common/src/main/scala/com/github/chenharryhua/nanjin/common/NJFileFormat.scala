package com.github.chenharryhua.nanjin.common

import cats.instances.int._
import enumeratum.values.{CatsOrderValueEnum, IntEnum, IntEnumEntry}
import monocle.Prism
import monocle.macros.GenPrism

import scala.collection.immutable

sealed abstract class NJFileFormat(val value: Int, val format: String, val suffix: String)
    extends IntEnumEntry with Serializable {
  final override def toString: String = format
}

object NJFileFormat extends CatsOrderValueEnum[Int, NJFileFormat] with IntEnum[NJFileFormat] {
  override val values: immutable.IndexedSeq[NJFileFormat] = findValues

  case object Json extends NJFileFormat(0, "json", ".json")
  //a variation of json
  case object Jackson extends NJFileFormat(1, "jackson", ".json")
  case object Parquet extends NJFileFormat(2, "parquet", ".parquet")
  case object Avro extends NJFileFormat(3, "avro", ".avro")
  case object Text extends NJFileFormat(4, "text", ".txt")

  type Json    = Json.type
  type Jackson = Jackson.type
  type Parquet = Parquet.type
  type Avro    = Avro.type
  type Text    = Text.type

  implicit val prismJson: Prism[NJFileFormat, Json] =
    GenPrism[NJFileFormat, Json]

  implicit val prismJackson: Prism[NJFileFormat, Jackson] =
    GenPrism[NJFileFormat, Jackson]

  implicit val prismParquet: Prism[NJFileFormat, Parquet] =
    GenPrism[NJFileFormat, Parquet]

  implicit val prismAvro: Prism[NJFileFormat, Avro] =
    GenPrism[NJFileFormat, Avro]

  implicit val prismText: Prism[NJFileFormat, Text] =
    GenPrism[NJFileFormat, Text]
}

trait UpdateParams[A, B] {
  def withParamUpdate(f: A => A): B
}
