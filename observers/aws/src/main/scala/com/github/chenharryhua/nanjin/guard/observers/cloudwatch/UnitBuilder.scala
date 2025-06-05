package com.github.chenharryhua.nanjin.guard.observers.cloudwatch

import squants.UnitOfMeasure
import squants.information.{DataRate, Information}
import squants.time.Time

final class UnitBuilder private[cloudwatch] (
  private[cloudwatch] val timeU: UnitOfMeasure[Time],
  private[cloudwatch] val dataRateU: UnitOfMeasure[DataRate],
  private[cloudwatch] val infoU: UnitOfMeasure[Information]) {

  def timeUnit(timeU: UnitOfMeasure[Time]): UnitBuilder =
    new UnitBuilder(timeU, dataRateU, infoU)

  def infoUnit(infoU: UnitOfMeasure[Information]): UnitBuilder =
    new UnitBuilder(timeU, dataRateU, infoU)

  def rateUnit(dataRateU: UnitOfMeasure[DataRate]): UnitBuilder =
    new UnitBuilder(timeU, dataRateU, infoU)

}
