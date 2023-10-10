package com.github.chenharryhua.nanjin.guard.event

import squants.Dimensionless
import squants.information.{DataRate, Information}
import squants.time.{Time, TimeConversions}

import java.time.Duration as JavaDuration
import scala.concurrent.duration.Duration
import scala.jdk.DurationConverters.JavaDurationOps

final case class Normalized(value: Double, by: MeasurementUnit)

final case class UnitNormalization(
  timeUnit: NJTimeUnit,
  infoUnit: Option[NJInformationUnit],
  rateUnit: Option[NJDataRateUnit]) {
  def normalize[A: Numeric](mu: MeasurementUnit, data: A): Normalized = mu match {
    case unit: NJTimeUnit =>
      val md: Time = unit.mUnit(data)
      Normalized(md.to(timeUnit.mUnit), timeUnit)
    case unit: NJInformationUnit =>
      val md: Information = unit.mUnit(data)
      infoUnit.map(base => Normalized(md.to(base.mUnit), base)).getOrElse(Normalized(md.value, mu))
    case unit: NJDataRateUnit =>
      val md: DataRate = unit.mUnit(data)
      rateUnit.map(base => Normalized(md.to(base.mUnit), base)).getOrElse(Normalized(md.value, mu))
    case unit: NJDimensionlessUnit =>
      val md: Dimensionless = unit.mUnit(data)
      Normalized(md.value, unit)
  }

  def normalize(dur: Duration): Normalized = {
    val md: Time = TimeConversions.scalaDurationToTime(dur)
    Normalized(md.to(timeUnit.mUnit), timeUnit)
  }

  def normalize(dur: JavaDuration): Normalized = normalize(dur.toScala)
}
