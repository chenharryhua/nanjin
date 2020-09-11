package com.github.chenharryhua.nanjin.codec.avro

import org.apache.avro.{LogicalType, LogicalTypes}
import shapeless.{:+:, CNil, Poly1}

// https://avro.apache.org/docs/1.10.0/spec.html
object NJLogicalTypes {

  sealed trait NJLogicalType {
    val logicalType: LogicalType
  }

  final case class NJDecimal(precision: Int, scale: Int) extends NJLogicalType {
    override val logicalType: LogicalType = LogicalTypes.decimal(precision, scale)
  }

  case object NJUuid extends NJLogicalType {
    override val logicalType: LogicalType = LogicalTypes.uuid()
  }

  case object NJDate extends NJLogicalType {
    override val logicalType: LogicalType = LogicalTypes.date()
  }

  case object NJTimeMillisecond extends NJLogicalType {
    override val logicalType: LogicalType = LogicalTypes.timeMillis()
  }

  case object NJTimeMicrosecond extends NJLogicalType {
    override val logicalType: LogicalType = LogicalTypes.timeMicros()
  }

  case object NJTimestampMillisecond extends NJLogicalType {
    override val logicalType: LogicalType = LogicalTypes.timestampMillis()
  }

  case object NJTimestampMicrosecond extends NJLogicalType {
    override val logicalType: LogicalType = LogicalTypes.timestampMicros()
  }

  case object NJLocalTimestampMillisecond extends NJLogicalType {
    override val logicalType: LogicalType = LogicalTypes.localTimestampMillis()
  }

  case object NJLocalTimestampMicrosecond extends NJLogicalType {
    override val logicalType: LogicalType = LogicalTypes.localTimestampMicros()
  }

  case object NJDuration extends NJLogicalType {
    override val logicalType: LogicalType = new LogicalType("duration")
    val size: Int                         = 12
  }

  type AnnLong = NJTimeMicrosecond.type :+:
    NJTimestampMillisecond.type :+:
    NJTimestampMicrosecond.type :+:
    NJLocalTimestampMillisecond.type :+:
    NJLocalTimestampMicrosecond.type :+:
    CNil

  private[avro] object longLT extends Poly1 {

    implicit val timeMicro: Case.Aux[NJTimeMicrosecond.type, LogicalType] =
      at[NJTimeMicrosecond.type](_.logicalType)

    implicit val tsMilli: Case.Aux[NJTimestampMillisecond.type, LogicalType] =
      at[NJTimestampMillisecond.type](_.logicalType)

    implicit val tsMicro: Case.Aux[NJTimestampMicrosecond.type, LogicalType] =
      at[NJTimestampMicrosecond.type](_.logicalType)

    implicit val localTsMilli: Case.Aux[NJLocalTimestampMillisecond.type, LogicalType] =
      at[NJLocalTimestampMillisecond.type](_.logicalType)

    implicit val localTsMicro: Case.Aux[NJLocalTimestampMicrosecond.type, LogicalType] =
      at[NJLocalTimestampMicrosecond.type](_.logicalType)
  }

  type AnnBytes = NJDecimal :+: CNil

  private[avro] object bytesLT extends Poly1 {

    implicit val decimal: Case.Aux[NJDecimal, LogicalType] =
      at[NJDecimal](_.logicalType)
  }

  type AnnFixed = NJDecimal :+: NJDuration.type :+: CNil

  private[avro] object fixedLT extends Poly1 {
    implicit val decimal: Case.Aux[NJDecimal, LogicalType] = at[NJDecimal](_.logicalType)

    implicit val duration: Case.Aux[NJDuration.type, LogicalType] =
      at[NJDuration.type](_.logicalType)

  }

  type AnnInt = NJDate.type :+: NJTimeMillisecond.type :+: CNil

  private[avro] object intLT extends Poly1 {
    implicit val date: Case.Aux[NJDate.type, LogicalType] = at[NJDate.type](_.logicalType)

    implicit val timeMilli: Case.Aux[NJTimeMillisecond.type, LogicalType] =
      at[NJTimeMillisecond.type](_.logicalType)
  }

  type AnnString = NJUuid.type :+: CNil

  private[avro] object stringLT extends Poly1 {

    implicit val decimal: Case.Aux[NJUuid.type, LogicalType] =
      at[NJUuid.type](_.logicalType)
  }

}
