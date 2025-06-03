package com.github.chenharryhua.nanjin.guard.observers.cloudwatch

import com.github.chenharryhua.nanjin.guard.event.MeasurementUnit
import com.github.chenharryhua.nanjin.guard.event.MeasurementUnit.{
  NJDataRateUnit,
  NJDimensionlessUnit,
  NJInformationUnit,
  NJTimeUnit
}
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

object CloudWatchTimeUnit {
  val MICROSECONDS: NJTimeUnit.MICROSECONDS.type = NJTimeUnit.MICROSECONDS
  val MILLISECONDS: NJTimeUnit.MILLISECONDS.type = NJTimeUnit.MILLISECONDS
  val SECONDS: NJTimeUnit.SECONDS.type = NJTimeUnit.SECONDS

  def toStandardUnit(mu: MeasurementUnit): StandardUnit =
    mu match {
      case NJTimeUnit.SECONDS              => StandardUnit.SECONDS
      case NJTimeUnit.MILLISECONDS         => StandardUnit.MILLISECONDS
      case NJTimeUnit.MICROSECONDS         => StandardUnit.MICROSECONDS
      case NJInformationUnit.BYTES         => StandardUnit.BYTES
      case NJInformationUnit.KILOBYTES     => StandardUnit.KILOBYTES
      case NJInformationUnit.MEGABYTES     => StandardUnit.MEGABYTES
      case NJInformationUnit.GIGABYTES     => StandardUnit.GIGABYTES
      case NJInformationUnit.TERABYTES     => StandardUnit.TERABYTES
      case NJInformationUnit.BITS          => StandardUnit.BITS
      case NJInformationUnit.KILOBITS      => StandardUnit.KILOBITS
      case NJInformationUnit.MEGABITS      => StandardUnit.MEGABITS
      case NJInformationUnit.GIGABITS      => StandardUnit.GIGABITS
      case NJInformationUnit.TERABITS      => StandardUnit.TERABITS
      case NJDimensionlessUnit.COUNT       => StandardUnit.COUNT
      case NJDataRateUnit.BYTES_SECOND     => StandardUnit.BYTES_SECOND
      case NJDataRateUnit.KILOBYTES_SECOND => StandardUnit.KILOBYTES_SECOND
      case NJDataRateUnit.MEGABYTES_SECOND => StandardUnit.MEGABYTES_SECOND
      case NJDataRateUnit.GIGABYTES_SECOND => StandardUnit.GIGABYTES_SECOND
      case NJDataRateUnit.TERABYTES_SECOND => StandardUnit.TERABYTES_SECOND
      case NJDataRateUnit.BITS_SECOND      => StandardUnit.BITS_SECOND
      case NJDataRateUnit.KILOBITS_SECOND  => StandardUnit.KILOBITS_SECOND
      case NJDataRateUnit.MEGABITS_SECOND  => StandardUnit.MEGABITS_SECOND
      case NJDataRateUnit.GIGABITS_SECOND  => StandardUnit.GIGABITS_SECOND
      case NJDataRateUnit.TERABITS_SECOND  => StandardUnit.TERABITS_SECOND

      case _ => StandardUnit.NONE
    }
}
