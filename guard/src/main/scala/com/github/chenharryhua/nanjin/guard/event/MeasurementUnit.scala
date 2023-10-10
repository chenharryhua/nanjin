package com.github.chenharryhua.nanjin.guard.event

import enumeratum.EnumEntry
import io.circe.generic.JsonCodec
import squants.information.*
import squants.time.*
import squants.{Dimensionless, DimensionlessUnit, Each, Percent, Quantity, UnitOfMeasure}

// consistent with software.amazon.awssdk.services.cloudwatch.model.StandardUnit

@JsonCodec
sealed trait MeasurementUnit extends Product with Serializable {
  type Q <: Quantity[Q]
  val mUnit: UnitOfMeasure[Q] {}
  final val symbol: String = mUnit.symbol
}

object MeasurementUnit {
  val DAYS: NJTimeUnit.DAYS.type                 = NJTimeUnit.DAYS
  val HOURS: NJTimeUnit.HOURS.type               = NJTimeUnit.HOURS
  val MINUTES: NJTimeUnit.MINUTES.type           = NJTimeUnit.MINUTES
  val SECONDS: NJTimeUnit.SECONDS.type           = NJTimeUnit.SECONDS
  val MILLISECONDS: NJTimeUnit.MILLISECONDS.type = NJTimeUnit.MILLISECONDS
  val MICROSECONDS: NJTimeUnit.MICROSECONDS.type = NJTimeUnit.MICROSECONDS
  val NANOSECONDS: NJTimeUnit.NANOSECONDS.type   = NJTimeUnit.NANOSECONDS

  val BYTES: NJInformationUnit.BYTES.type         = NJInformationUnit.BYTES
  val KILOBYTES: NJInformationUnit.KILOBYTES.type = NJInformationUnit.KILOBYTES
  val MEGABYTES: NJInformationUnit.MEGABYTES.type = NJInformationUnit.MEGABYTES
  val GIGABYTES: NJInformationUnit.GIGABYTES.type = NJInformationUnit.GIGABYTES
  val TERABYTES: NJInformationUnit.TERABYTES.type = NJInformationUnit.TERABYTES

  val BITS: NJInformationUnit.BITS.type         = NJInformationUnit.BITS
  val KILOBITS: NJInformationUnit.KILOBITS.type = NJInformationUnit.KILOBITS
  val MEGABITS: NJInformationUnit.MEGABITS.type = NJInformationUnit.MEGABITS
  val GIGABITS: NJInformationUnit.GIGABITS.type = NJInformationUnit.GIGABITS
  val TERABITS: NJInformationUnit.TERABITS.type = NJInformationUnit.TERABITS

  val BYTES_SECOND: NJDataRateUnit.BYTES_SECOND.type         = NJDataRateUnit.BYTES_SECOND
  val KILOBYTES_SECOND: NJDataRateUnit.KILOBYTES_SECOND.type = NJDataRateUnit.KILOBYTES_SECOND
  val MEGABYTES_SECOND: NJDataRateUnit.MEGABYTES_SECOND.type = NJDataRateUnit.MEGABYTES_SECOND
  val GIGABYTES_SECOND: NJDataRateUnit.GIGABYTES_SECOND.type = NJDataRateUnit.GIGABYTES_SECOND
  val TERABYTES_SECOND: NJDataRateUnit.TERABYTES_SECOND.type = NJDataRateUnit.TERABYTES_SECOND

  val BITS_SECOND: NJDataRateUnit.BITS_SECOND.type         = NJDataRateUnit.BITS_SECOND
  val KILOBITS_SECOND: NJDataRateUnit.KILOBITS_SECOND.type = NJDataRateUnit.KILOBITS_SECOND
  val MEGABITS_SECOND: NJDataRateUnit.MEGABITS_SECOND.type = NJDataRateUnit.MEGABITS_SECOND
  val GIGABITS_SECOND: NJDataRateUnit.GIGABITS_SECOND.type = NJDataRateUnit.GIGABITS_SECOND
  val TERABITS_SECOND: NJDataRateUnit.TERABITS_SECOND.type = NJDataRateUnit.TERABITS_SECOND

  val COUNT: NJDimensionlessUnit.COUNT.type = NJDimensionlessUnit.COUNT
  val PERCENT: NJDimensionlessUnit.PERCENT.type = NJDimensionlessUnit.PERCENT
}

sealed abstract class NJTimeUnit(val mUnit: TimeUnit) extends MeasurementUnit with EnumEntry {
  type Q = Time
}

object NJTimeUnit extends enumeratum.Enum[NJTimeUnit] {
  val values: IndexedSeq[NJTimeUnit] = findValues
  case object DAYS extends NJTimeUnit(Days)
  case object HOURS extends NJTimeUnit(Hours)
  case object MINUTES extends NJTimeUnit(Minutes)
  case object SECONDS extends NJTimeUnit(Seconds)
  case object MILLISECONDS extends NJTimeUnit(Milliseconds)
  case object MICROSECONDS extends NJTimeUnit(Microseconds)
  case object NANOSECONDS extends NJTimeUnit(Nanoseconds)
}

sealed abstract class NJInformationUnit(val mUnit: InformationUnit) extends MeasurementUnit with EnumEntry {
  type Q = Information
}

object NJInformationUnit extends enumeratum.Enum[NJInformationUnit] {
  val values: IndexedSeq[NJInformationUnit] = findValues
  case object BYTES extends NJInformationUnit(Bytes)
  case object KILOBYTES extends NJInformationUnit(Kilobytes)
  case object MEGABYTES extends NJInformationUnit(Megabytes)
  case object GIGABYTES extends NJInformationUnit(Gigabytes)
  case object TERABYTES extends NJInformationUnit(Terabytes)

  case object BITS extends NJInformationUnit(Bits)
  case object KILOBITS extends NJInformationUnit(Kilobits)
  case object MEGABITS extends NJInformationUnit(Megabits)
  case object GIGABITS extends NJInformationUnit(Gigabits)
  case object TERABITS extends NJInformationUnit(Terabits)
}

sealed abstract class NJDataRateUnit(val mUnit: DataRateUnit) extends MeasurementUnit with EnumEntry {
  type Q = DataRate
}
object NJDataRateUnit extends enumeratum.Enum[NJDataRateUnit] {
  val values: IndexedSeq[NJDataRateUnit] = findValues
  case object BYTES_SECOND extends NJDataRateUnit(BytesPerSecond)
  case object KILOBYTES_SECOND extends NJDataRateUnit(KilobytesPerSecond)
  case object MEGABYTES_SECOND extends NJDataRateUnit(MegabytesPerSecond)
  case object GIGABYTES_SECOND extends NJDataRateUnit(GigabytesPerSecond)
  case object TERABYTES_SECOND extends NJDataRateUnit(TerabytesPerSecond)

  case object BITS_SECOND extends NJDataRateUnit(BitsPerSecond)
  case object KILOBITS_SECOND extends NJDataRateUnit(KilobitsPerSecond)
  case object MEGABITS_SECOND extends NJDataRateUnit(MegabitsPerSecond)
  case object GIGABITS_SECOND extends NJDataRateUnit(GigabitsPerSecond)
  case object TERABITS_SECOND extends NJDataRateUnit(TerabitsPerSecond)
}

sealed abstract class NJDimensionlessUnit(val mUnit: DimensionlessUnit)
    extends MeasurementUnit with EnumEntry {
  type Q = Dimensionless
}

object NJDimensionlessUnit extends enumeratum.Enum[NJDimensionlessUnit] {
  val values: IndexedSeq[NJDimensionlessUnit] = findValues

  case object COUNT extends NJDimensionlessUnit(Each)
  case object PERCENT extends NJDimensionlessUnit(Percent)
}
