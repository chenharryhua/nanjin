package com.github.chenharryhua.nanjin.codec.avro

import shapeless.{:+:, CNil}

// https://avro.apache.org/docs/1.10.0/spec.html
object NJLogicalTypes {

  sealed trait NJLogicalType {
    val logicalType: String
  }

  final case class NJDecimal(precision: Int, scale: Int) extends NJLogicalType {
    override val logicalType: String = "decimal"
  }

  case object NJUuid extends NJLogicalType {
    override val logicalType: String = "uuid"
  }

  case object NJDate extends NJLogicalType {
    override val logicalType: String = "date"
  }

  case object NJTimeMillisecond extends NJLogicalType {
    override val logicalType: String = "time-millis"
  }

  case object NJTimeMicrosecond extends NJLogicalType {
    override val logicalType: String = "time-micros"
  }

  case object NJTimestampMillisecond extends NJLogicalType {
    override val logicalType: String = "timestamp-millis"
  }

  case object NJTimestampMicrosecond extends NJLogicalType {
    override val logicalType: String = "timestamp-micros"
  }

  case object NJLocalTimestampMillisecond extends NJLogicalType {
    override val logicalType: String = "local-timestamp-millis"
  }

  case object NJLocalTimestampMicrosecond extends NJLogicalType {
    override val logicalType: String = "local-timestamp-micros"
  }

  case object NJDuration extends NJLogicalType {
    override val logicalType: String = "duration"

    val size: Int = 12
  }

  type AnnLong = NJTimeMicrosecond.type :+:
    NJTimestampMillisecond.type :+:
    NJTimestampMicrosecond.type :+:
    NJLocalTimestampMillisecond.type :+:
    NJLocalTimestampMicrosecond.type :+:
    CNil
  type AnnBytes  = NJDecimal :+: CNil
  type AnnFixed  = NJDecimal :+: NJDuration.type :+: CNil
  type AnnInt    = NJDate.type :+: NJTimeMillisecond.type :+: CNil
  type AnnString = NJUuid.type :+: CNil

}
