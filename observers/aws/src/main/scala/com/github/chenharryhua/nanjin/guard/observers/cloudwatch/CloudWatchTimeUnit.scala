package com.github.chenharryhua.nanjin.guard.observers.cloudwatch

import software.amazon.awssdk.services.cloudwatch.model.StandardUnit
import squants.{information, time, Each}

object CloudWatchTimeUnit {

  def toStandardUnit(unitBuilder: UnitBuilder, mu: String, data: Double): (StandardUnit, Double) =
    mu match {
      case Each.symbol =>
        (StandardUnit.COUNT, data)

      // time
      case time.Seconds.symbol =>
        (StandardUnit.SECONDS, time.Seconds(data).to(unitBuilder.timeU))
      case time.Milliseconds.symbol =>
        (StandardUnit.MILLISECONDS, time.Milliseconds(data).to(unitBuilder.timeU))
      case time.Microseconds.symbol =>
        (StandardUnit.MICROSECONDS, time.Microseconds(data).to(unitBuilder.timeU))

      // info
      case information.Bytes.symbol =>
        (StandardUnit.BYTES, information.Bytes(data).to(unitBuilder.infoU))
      case information.Kilobytes.symbol =>
        (StandardUnit.KILOBYTES, information.Kilobytes(data).to(unitBuilder.infoU))
      case information.Megabytes.symbol =>
        (StandardUnit.MEGABYTES, information.Megabytes(data).to(unitBuilder.infoU))
      case information.Gigabytes.symbol =>
        (StandardUnit.GIGABYTES, information.Gigabytes(data).to(unitBuilder.infoU))
      case information.Terabytes.symbol =>
        (StandardUnit.TERABYTES, information.Terabytes(data).to(unitBuilder.infoU))
      case information.Bits.symbol =>
        (StandardUnit.BITS, information.Bits(data).to(unitBuilder.infoU))
      case information.Kilobits.symbol =>
        (StandardUnit.KILOBITS, information.Kilobits(data).to(unitBuilder.infoU))
      case information.Megabits.symbol =>
        (StandardUnit.MEGABITS, information.Megabits(data).to(unitBuilder.infoU))
      case information.Gigabits.symbol =>
        (StandardUnit.GIGABITS, information.Gigabits(data).to(unitBuilder.infoU))
      case information.Terabits.symbol =>
        (StandardUnit.TERABITS, information.Terabits(data).to(unitBuilder.infoU))

      // rate
      case information.BytesPerSecond.symbol =>
        (StandardUnit.BYTES_SECOND, information.BytesPerSecond(data).to(unitBuilder.dataRateU))
      case information.KilobytesPerSecond.symbol =>
        (StandardUnit.KILOBYTES_SECOND, information.KilobytesPerSecond(data).to(unitBuilder.dataRateU))
      case information.MegabytesPerSecond.symbol =>
        (StandardUnit.MEGABYTES_SECOND, information.MegabytesPerSecond(data).to(unitBuilder.dataRateU))
      case information.GigabytesPerSecond.symbol =>
        (StandardUnit.GIGABYTES_SECOND, information.GigabytesPerSecond(data).to(unitBuilder.dataRateU))
      case information.TerabytesPerSecond.symbol =>
        (StandardUnit.TERABYTES_SECOND, information.TerabytesPerSecond(data).to(unitBuilder.dataRateU))
      case information.BitsPerSecond.symbol =>
        (StandardUnit.BITS_SECOND, information.BitsPerSecond(data).to(unitBuilder.dataRateU))
      case information.KilobitsPerSecond.symbol =>
        (StandardUnit.KILOBITS_SECOND, information.KilobitsPerSecond(data).to(unitBuilder.dataRateU))
      case information.MegabitsPerSecond.symbol =>
        (StandardUnit.MEGABITS_SECOND, information.MegabitsPerSecond(data).to(unitBuilder.dataRateU))
      case information.GigabitsPerSecond.symbol =>
        (StandardUnit.GIGABITS_SECOND, information.GigabitsPerSecond(data).to(unitBuilder.dataRateU))
      case information.TerabitsPerSecond.symbol =>
        (StandardUnit.TERABITS_SECOND, information.TerabitsPerSecond(data).to(unitBuilder.dataRateU))

      case _ => (StandardUnit.NONE, data)
    }
}
