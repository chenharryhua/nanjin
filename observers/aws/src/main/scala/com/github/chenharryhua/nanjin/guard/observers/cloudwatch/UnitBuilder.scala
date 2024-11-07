package com.github.chenharryhua.nanjin.guard.observers.cloudwatch

import com.github.chenharryhua.nanjin.guard.event.MeasurementUnit.{
  NJDataRateUnit,
  NJInformationUnit,
  NJTimeUnit
}
import com.github.chenharryhua.nanjin.guard.event.UnitNormalization
import monocle.Monocle.toAppliedFocusOps

final class UnitBuilder private[cloudwatch] (unitNormalization: UnitNormalization) {

  def withTimeUnit(f: CloudWatchTimeUnit.type => NJTimeUnit) =
    new UnitBuilder(unitNormalization.focus(_.timeUnit).replace(f(CloudWatchTimeUnit)))

  def withInfoUnit(f: NJInformationUnit.type => NJInformationUnit) =
    new UnitBuilder(unitNormalization.focus(_.infoUnit).replace(Some(f(NJInformationUnit))))

  def withRateUnit(f: NJDataRateUnit.type => NJDataRateUnit) =
    new UnitBuilder(unitNormalization.focus(_.rateUnit).replace(Some(f(NJDataRateUnit))))

  private[cloudwatch] def build: UnitNormalization = unitNormalization

}
