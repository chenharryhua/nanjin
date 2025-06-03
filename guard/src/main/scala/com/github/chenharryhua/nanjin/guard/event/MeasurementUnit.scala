package com.github.chenharryhua.nanjin.guard.event

import enumeratum.{CirceEnum, Enum, EnumEntry}
import squants.information.*
import squants.time.*
import squants.{Dimensionless, DimensionlessUnit, Each, Percent, Quantity, UnitOfMeasure}

// consistent with software.amazon.awssdk.services.cloudwatch.model.StandardUnit

sealed trait MeasurementUnit extends Product with Serializable with EnumEntry {
  type Q <: Quantity[Q]
  val mUnit: UnitOfMeasure[Q] {}
  final val symbol: String = mUnit.symbol
}

object MeasurementUnit extends Enum[MeasurementUnit] with CirceEnum[MeasurementUnit] {
  val values: IndexedSeq[MeasurementUnit] = findValues

  sealed abstract class NJTimeUnit(val mUnit: TimeUnit) extends MeasurementUnit {
    type Q = Time
  }

  object NJTimeUnit extends Enum[NJTimeUnit] {
    val values: IndexedSeq[NJTimeUnit] = findValues

    case object DAYS extends NJTimeUnit(Days)
    case object HOURS extends NJTimeUnit(Hours)
    case object MINUTES extends NJTimeUnit(Minutes)
    case object SECONDS extends NJTimeUnit(Seconds)
    case object MILLISECONDS extends NJTimeUnit(Milliseconds)
    case object MICROSECONDS extends NJTimeUnit(Microseconds)
    case object NANOSECONDS extends NJTimeUnit(Nanoseconds)
  }

  sealed abstract class NJInformationUnit(val mUnit: InformationUnit) extends MeasurementUnit {
    type Q = Information
  }

  object NJInformationUnit extends Enum[NJInformationUnit] {
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

  sealed abstract class NJDataRateUnit(val mUnit: DataRateUnit) extends MeasurementUnit {
    type Q = DataRate
  }

  object NJDataRateUnit extends Enum[NJDataRateUnit] {
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

  sealed abstract class NJDimensionlessUnit(val mUnit: DimensionlessUnit) extends MeasurementUnit {
    type Q = Dimensionless
  }

  object NJDimensionlessUnit extends Enum[NJDimensionlessUnit] {
    val values: IndexedSeq[NJDimensionlessUnit] = findValues

    case object COUNT extends NJDimensionlessUnit(Each)
    case object PERCENT extends NJDimensionlessUnit(Percent)
  }
}

object NJUnits {
  import MeasurementUnit.*
  val DAYS: NJTimeUnit = NJTimeUnit.DAYS
  val HOURS: NJTimeUnit = NJTimeUnit.HOURS
  val MINUTES: NJTimeUnit = NJTimeUnit.MINUTES
  val SECONDS: NJTimeUnit = NJTimeUnit.SECONDS
  val MILLISECONDS: NJTimeUnit = NJTimeUnit.MILLISECONDS
  val MICROSECONDS: NJTimeUnit = NJTimeUnit.MICROSECONDS
  val NANOSECONDS: NJTimeUnit = NJTimeUnit.NANOSECONDS

  val BYTES: NJInformationUnit = NJInformationUnit.BYTES
  val KILOBYTES: NJInformationUnit = NJInformationUnit.KILOBYTES
  val MEGABYTES: NJInformationUnit = NJInformationUnit.MEGABYTES
  val GIGABYTES: NJInformationUnit = NJInformationUnit.GIGABYTES
  val TERABYTES: NJInformationUnit = NJInformationUnit.TERABYTES

  val BITS: NJInformationUnit = NJInformationUnit.BITS
  val KILOBITS: NJInformationUnit = NJInformationUnit.KILOBITS
  val MEGABITS: NJInformationUnit = NJInformationUnit.MEGABITS
  val GIGABITS: NJInformationUnit = NJInformationUnit.GIGABITS
  val TERABITS: NJInformationUnit = NJInformationUnit.TERABITS

  val BYTES_SECOND: NJDataRateUnit = NJDataRateUnit.BYTES_SECOND
  val KILOBYTES_SECOND: NJDataRateUnit = NJDataRateUnit.KILOBYTES_SECOND
  val MEGABYTES_SECOND: NJDataRateUnit = NJDataRateUnit.MEGABYTES_SECOND
  val GIGABYTES_SECOND: NJDataRateUnit = NJDataRateUnit.GIGABYTES_SECOND
  val TERABYTES_SECOND: NJDataRateUnit = NJDataRateUnit.TERABYTES_SECOND

  val BITS_SECOND: NJDataRateUnit = NJDataRateUnit.BITS_SECOND
  val KILOBITS_SECOND: NJDataRateUnit = NJDataRateUnit.KILOBITS_SECOND
  val MEGABITS_SECOND: NJDataRateUnit = NJDataRateUnit.MEGABITS_SECOND
  val GIGABITS_SECOND: NJDataRateUnit = NJDataRateUnit.GIGABITS_SECOND
  val TERABITS_SECOND: NJDataRateUnit = NJDataRateUnit.TERABITS_SECOND

  val COUNT: NJDimensionlessUnit = NJDimensionlessUnit.COUNT
  val PERCENT: NJDimensionlessUnit = NJDimensionlessUnit.PERCENT
}
