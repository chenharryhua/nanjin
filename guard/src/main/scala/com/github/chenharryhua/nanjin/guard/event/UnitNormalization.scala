package com.github.chenharryhua.nanjin.guard.event

final case class Normalized(value: Double, unit: MeasurementUnit)

final case class UnitNormalization(
  timeUnit: NJTimeUnit,
  infoUnit: NJInformationUnit,
  rateUnit: NJDataRateUnit) {
  def normalize[A: Numeric](mu: MeasurementUnit, data: A): Normalized = mu match {
    case unit: NJTimeUnit          => Normalized(unit.mUnit(data).in(timeUnit.mUnit).value, timeUnit)
    case unit: NJInformationUnit   => Normalized(unit.mUnit(data).in(infoUnit.mUnit).value, infoUnit)
    case unit: NJDataRateUnit      => Normalized(unit.mUnit(data).in(rateUnit.mUnit).value, rateUnit)
    case unit: NJDimensionlessUnit => Normalized(unit.mUnit(data).value, NJDimensionlessUnit.COUNT)
  }
}
