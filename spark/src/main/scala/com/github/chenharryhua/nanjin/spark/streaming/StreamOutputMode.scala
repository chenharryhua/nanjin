package com.github.chenharryhua.nanjin.spark.streaming

import enumeratum.{CatsEnum, Enum, EnumEntry}
import shapeless._

import scala.collection.immutable

//http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes

sealed abstract class StreamOutputMode(val value: String) extends EnumEntry

object StreamOutputMode extends Enum[StreamOutputMode] with CatsEnum[StreamOutputMode] {
  override val values: immutable.IndexedSeq[StreamOutputMode] = findValues
  case object Append extends StreamOutputMode("append")
  case object Update extends StreamOutputMode("update")
  case object Complete extends StreamOutputMode("complete")

  type Append   = Append.type
  type Update   = Update.type
  type Complete = Complete.type

  type FullMode   = Append :+: Update :+: Complete :+: CNil
  type MemoryMode = Append :+: Complete :+: CNil
}
