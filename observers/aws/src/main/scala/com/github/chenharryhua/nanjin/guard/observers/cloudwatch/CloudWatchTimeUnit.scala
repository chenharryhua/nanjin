package com.github.chenharryhua.nanjin.guard.observers.cloudwatch

import cats.implicits.catsSyntaxEq
import com.github.chenharryhua.nanjin.guard.config.Squants
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit
import squants.{information, time, Dimensionless, Dozen, Each, Gross, Percent, Score}

object CloudWatchTimeUnit {

  def toStandardUnit(squants: Squants, data: Double): (StandardUnit, Double) = {
    val Squants(unitSymbol, dimensionName) = squants
    unitSymbol match {
      case Each.symbol if dimensionName === Dimensionless.name =>
        (StandardUnit.COUNT, data)
      case Dozen.symbol if dimensionName === Dimensionless.name =>
        (StandardUnit.COUNT, data * Dozen.conversionFactor)
      case Score.symbol if dimensionName === Dimensionless.name =>
        (StandardUnit.COUNT, data * Score.conversionFactor)
      case Gross.symbol if dimensionName === Dimensionless.name =>
        (StandardUnit.COUNT, data * Gross.conversionFactor)
      case Percent.symbol if dimensionName === Dimensionless.name =>
        (StandardUnit.PERCENT, data)

      // time
      case time.Seconds.symbol if dimensionName === time.Time.name =>
        (StandardUnit.SECONDS, data)
      case time.Milliseconds.symbol if dimensionName === time.Time.name =>
        (StandardUnit.MILLISECONDS, data)
      case time.Microseconds.symbol if dimensionName === time.Time.name =>
        (StandardUnit.MICROSECONDS, data)
      case time.Nanoseconds.symbol if dimensionName === time.Time.name =>
        (StandardUnit.MICROSECONDS, data / 1000)

      // info bytes
      case information.Bytes.symbol if dimensionName === information.Information.name =>
        (StandardUnit.BYTES, data)
      case information.Octets.symbol if dimensionName === information.Information.name =>
        (StandardUnit.BYTES, data)

      case information.Kilobytes.symbol if dimensionName === information.Information.name =>
        (StandardUnit.KILOBYTES, data)
      case information.Kibibytes.symbol if dimensionName === information.Information.name =>
        (StandardUnit.KILOBYTES, information.Kibibytes(data).to(information.Kilobytes))

      case information.Megabytes.symbol if dimensionName === information.Information.name =>
        (StandardUnit.MEGABYTES, data)
      case information.Mebibytes.symbol if dimensionName === information.Information.name =>
        (StandardUnit.MEGABYTES, information.Mebibytes(data).to(information.Megabytes))

      case information.Gigabytes.symbol if dimensionName === information.Information.name =>
        (StandardUnit.GIGABYTES, data)
      case information.Gibibytes.symbol if dimensionName === information.Information.name =>
        (StandardUnit.GIGABYTES, information.Gibibytes(data).to(information.Gigabytes))

      case information.Terabytes.symbol if dimensionName === information.Information.name =>
        (StandardUnit.TERABYTES, data)
      case information.Tebibytes.symbol if dimensionName === information.Information.name =>
        (StandardUnit.TERABYTES, information.Tebibytes(data).to(information.Terabytes))

      // info bits
      case information.Bits.symbol if dimensionName === information.Information.name =>
        (StandardUnit.BITS, data)
      case information.Kilobits.symbol if dimensionName === information.Information.name =>
        (StandardUnit.KILOBITS, data)
      case information.Kibibits.symbol if dimensionName === information.Information.name =>
        (StandardUnit.KILOBITS, information.Kibibits(data).to(information.Kilobits))

      case information.Megabits.symbol if dimensionName === information.Information.name =>
        (StandardUnit.MEGABITS, data)
      case information.Mebibits.symbol if dimensionName === information.Information.name =>
        (StandardUnit.MEGABITS, information.Mebibits(data).to(information.Megabits))

      case information.Gigabits.symbol if dimensionName === information.Information.name =>
        (StandardUnit.GIGABITS, data)
      case information.Gibibits.symbol if dimensionName === information.Information.name =>
        (StandardUnit.GIGABITS, information.Gibibits(data).to(information.Gigabits))

      case information.Terabits.symbol if dimensionName === information.Information.name =>
        (StandardUnit.TERABITS, data)
      case information.Tebibits.symbol if dimensionName === information.Information.name =>
        (StandardUnit.TERABITS, information.Tebibits(data).to(information.Terabits))

      // rate bytes/second
      case information.BytesPerSecond.symbol if dimensionName === information.DataRate.name =>
        (StandardUnit.BYTES_SECOND, data)

      case information.KilobytesPerSecond.symbol if dimensionName === information.DataRate.name =>
        (StandardUnit.KILOBYTES_SECOND, data)
      case information.KibibytesPerSecond.symbol if dimensionName === information.DataRate.name =>
        (
          StandardUnit.KILOBYTES_SECOND,
          information.KibibytesPerSecond(data).to(information.KilobytesPerSecond))

      case information.MegabytesPerSecond.symbol if dimensionName === information.DataRate.name =>
        (StandardUnit.MEGABYTES_SECOND, data)
      case information.MebibytesPerSecond.symbol if dimensionName === information.DataRate.name =>
        (
          StandardUnit.MEGABYTES_SECOND,
          information.MebibytesPerSecond(data).to(information.MegabytesPerSecond))

      case information.GigabytesPerSecond.symbol if dimensionName === information.DataRate.name =>
        (StandardUnit.GIGABYTES_SECOND, data)
      case information.GibibytesPerSecond.symbol if dimensionName === information.DataRate.name =>
        (
          StandardUnit.GIGABYTES_SECOND,
          information.GibibytesPerSecond(data).to(information.GigabytesPerSecond))

      case information.TerabytesPerSecond.symbol if dimensionName === information.DataRate.name =>
        (StandardUnit.TERABYTES_SECOND, data)
      case information.TebibytesPerSecond.symbol if dimensionName === information.DataRate.name =>
        (
          StandardUnit.TERABYTES_SECOND,
          information.TebibytesPerSecond(data).to(information.TerabytesPerSecond))

      // rate bits/second
      case information.BitsPerSecond.symbol if dimensionName === information.DataRate.name =>
        (StandardUnit.BITS_SECOND, data)

      case information.KilobitsPerSecond.symbol if dimensionName === information.DataRate.name =>
        (StandardUnit.KILOBITS_SECOND, data)
      case information.KibibitsPerSecond.symbol if dimensionName === information.DataRate.name =>
        (StandardUnit.KILOBITS_SECOND, information.KibibitsPerSecond(data).to(information.KilobitsPerSecond))

      case information.MegabitsPerSecond.symbol if dimensionName === information.DataRate.name =>
        (StandardUnit.MEGABITS_SECOND, data)
      case information.MebibitsPerSecond.symbol if dimensionName === information.DataRate.name =>
        (StandardUnit.MEGABITS_SECOND, information.MebibitsPerSecond(data).to(information.MegabitsPerSecond))

      case information.GigabitsPerSecond.symbol if dimensionName === information.DataRate.name =>
        (StandardUnit.GIGABITS_SECOND, data)
      case information.GibibitsPerSecond.symbol if dimensionName === information.DataRate.name =>
        (StandardUnit.GIGABITS_SECOND, information.GibibitsPerSecond(data).to(information.GigabitsPerSecond))

      case information.TerabitsPerSecond.symbol if dimensionName === information.DataRate.name =>
        (StandardUnit.TERABITS_SECOND, data)
      case information.TebibitsPerSecond.symbol if dimensionName === information.DataRate.name =>
        (StandardUnit.TERABITS_SECOND, information.TebibitsPerSecond(data).to(information.TerabitsPerSecond))

      case _ => (StandardUnit.NONE, data)
    }
  }
}
