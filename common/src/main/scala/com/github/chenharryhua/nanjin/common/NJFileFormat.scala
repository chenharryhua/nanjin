package com.github.chenharryhua.nanjin.common

import cats.instances.int._
import enumeratum.values.{CatsOrderValueEnum, IntEnum, IntEnumEntry}
import monocle.Prism
import monocle.macros.GenPrism

import scala.collection.immutable

sealed abstract class NJFileFormat(val value: Int, val format: String)
    extends IntEnumEntry with Serializable {
  final override def toString: String = format
}

object NJFileFormat extends CatsOrderValueEnum[Int, NJFileFormat] with IntEnum[NJFileFormat] {
  override val values: immutable.IndexedSeq[NJFileFormat] = findValues

  case object Json extends NJFileFormat(0, "json")
  case object Jackson extends NJFileFormat(1, "jackson")
  case object Parquet extends NJFileFormat(2, "parquet")
  case object Avro extends NJFileFormat(3, "avro")

  type Json    = Json.type
  type Jackson = Jackson.type
  type Parquet = Parquet.type
  type Avro    = Avro.type

  implicit val prismJson: Prism[NJFileFormat, Json] =
    GenPrism[NJFileFormat, Json]

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
