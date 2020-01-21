package com.github.chenharryhua.nanjin.common

import cats.instances.int._
import enumeratum.values.{CatsOrderValueEnum, IntEnum, IntEnumEntry}

import scala.collection.immutable

sealed abstract class NJFileFormat(val value: Int, val format: String)
    extends IntEnumEntry with Serializable

object NJFileFormat extends CatsOrderValueEnum[Int, NJFileFormat] with IntEnum[NJFileFormat] {
  override val values: immutable.IndexedSeq[NJFileFormat] = findValues

  case object Json extends NJFileFormat(0, "json")
  case object Parquet extends NJFileFormat(1, "parquet")
  case object Avro extends NJFileFormat(2, "avro")

}

trait UpdateParams[A, B] {
  def updateParams(f: A => A): B
}
