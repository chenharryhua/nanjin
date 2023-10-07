package com.github.chenharryhua.nanjin.guard.event

import enumeratum.EnumEntry.Lowercase
import enumeratum.{CatsEnum, CirceEnum, EnumEntry}
import io.circe.generic.JsonCodec
import squants.information.*
import squants.time.*
import squants.{Dimensionless, DimensionlessUnit, Each, Percent, Quantity, UnitOfMeasure}

// consistent with software.amazon.awssdk.services.cloudwatch.model.StandardUnit

@JsonCodec
sealed trait MeasurementUnit extends EnumEntry {
  type Q <: Quantity[Q]
  def mUnit: UnitOfMeasure[Q]
  def symbol: String = mUnit.symbol
}

sealed abstract class NJTimeUnit(val mUnit: TimeUnit) extends MeasurementUnit {
  type Q = Time
}

object NJTimeUnit
    extends enumeratum.Enum[NJTimeUnit] with CirceEnum[NJTimeUnit] with CatsEnum[NJTimeUnit] with Lowercase {
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

object NJInformationUnit
    extends enumeratum.Enum[NJInformationUnit] with CirceEnum[NJInformationUnit]
    with CatsEnum[NJInformationUnit] with Lowercase {
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
object NJDataRateUnit
    extends enumeratum.Enum[NJDataRateUnit] with CirceEnum[NJDataRateUnit] with CatsEnum[NJDataRateUnit]
    with Lowercase {
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

object NJDimensionlessUnit
    extends enumeratum.Enum[NJDimensionlessUnit] with CirceEnum[NJDimensionlessUnit]
    with CatsEnum[NJDimensionlessUnit] with Lowercase {
  val values: IndexedSeq[NJDimensionlessUnit] = findValues

  case object PERCENT extends NJDimensionlessUnit(Percent)
  case object COUNT extends NJDimensionlessUnit(Each)
}
