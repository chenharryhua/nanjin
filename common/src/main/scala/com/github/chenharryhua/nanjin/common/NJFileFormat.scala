package com.github.chenharryhua.nanjin.common

import cats.instances.int._
import enumeratum.values.{CatsOrderValueEnum, IntEnum, IntEnumEntry}
import monocle.Prism
import monocle.macros.GenPrism

import scala.collection.immutable

sealed abstract class NJFileFormat(val value: Int, val format: String)
    extends IntEnumEntry with Serializable

object NJFileFormat extends CatsOrderValueEnum[Int, NJFileFormat] with IntEnum[NJFileFormat] {
  override val values: immutable.IndexedSeq[NJFileFormat] = findValues

  case object Json extends NJFileFormat(0, "json")
  case object Parquet extends NJFileFormat(1, "parquet")
  case object Avro extends NJFileFormat(2, "avro")

  implicit val prismJson: Prism[NJFileFormat, Json.type] =
    GenPrism[NJFileFormat, Json.type]

  implicit val prismParquet: Prism[NJFileFormat, Parquet.type] =
    GenPrism[NJFileFormat, Parquet.type]

  implicit val prismAvro: Prism[NJFileFormat, Avro.type] =
    GenPrism[NJFileFormat, Avro.type]
}

trait UpdateParams[A, B] {
  def updateParams(f: A => A): B
}
