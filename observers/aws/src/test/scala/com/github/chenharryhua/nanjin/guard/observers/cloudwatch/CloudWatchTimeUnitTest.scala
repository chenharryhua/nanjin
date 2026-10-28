package com.github.chenharryhua.nanjin.guard.observers.cloudwatch

import munit.FunSuite
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit
import squants.{information, time, Dimensionless, Dozen, Each, Gross, Percent, Score}

/** Tests for `CloudWatchTimeUnit.toStandardUnit`, the pure mapping from a squants unit symbol + dimension
  * name to a CloudWatch `StandardUnit` and a (possibly rescaled) value.
  *
  * The mapping only fires when both the unit symbol AND the dimension name match; a symbol paired with the
  * wrong dimension falls through to `NONE`. Binary units (Ki/Mi/...) are rescaled to their decimal CloudWatch
  * counterparts, and nanoseconds are folded into microseconds.
  */
class CloudWatchTimeUnitTest extends FunSuite {

  private def convert(unitSymbol: String, dimensionName: String, data: Double): (StandardUnit, Double) =
    CloudWatchTimeUnit.toStandardUnit(unitSymbol, dimensionName, data)

  // ---- dimensionless counts ------------------------------------------------------------------------

  test("1.Each maps to COUNT unchanged") {
    assert(convert(Each.symbol, Dimensionless.name, 5.0) == (StandardUnit.COUNT, 5.0))
  }

  test("2.Dozen/Score/Gross map to COUNT scaled by their conversion factor") {
    assert(convert(Dozen.symbol, Dimensionless.name, 2.0) == (StandardUnit.COUNT, 24.0))
    assert(convert(Score.symbol, Dimensionless.name, 2.0) == (StandardUnit.COUNT, 40.0))
    assert(convert(Gross.symbol, Dimensionless.name, 1.0) == (StandardUnit.COUNT, 144.0))
  }

  test("3.Percent maps to PERCENT unchanged") {
    assert(convert(Percent.symbol, Dimensionless.name, 42.0) == (StandardUnit.PERCENT, 42.0))
  }

  // ---- time ----------------------------------------------------------------------------------------

  test("4.time units map directly, unscaled") {
    assert(convert(time.Seconds.symbol, time.Time.name, 3.0) == (StandardUnit.SECONDS, 3.0))
    assert(convert(time.Milliseconds.symbol, time.Time.name, 3.0) == (StandardUnit.MILLISECONDS, 3.0))
    assert(convert(time.Microseconds.symbol, time.Time.name, 3.0) == (StandardUnit.MICROSECONDS, 3.0))
  }

  test("5.nanoseconds fold into microseconds (divided by 1000)") {
    assert(convert(time.Nanoseconds.symbol, time.Time.name, 5000.0) == (StandardUnit.MICROSECONDS, 5.0))
  }

  // ---- information: decimal units pass through, binary units rescale -------------------------------

  test("6.Bytes and Octets both map to BYTES unchanged") {
    assert(
      convert(information.Bytes.symbol, information.Information.name, 100.0) == (StandardUnit.BYTES, 100.0))
    assert(
      convert(information.Octets.symbol, information.Information.name, 100.0) == (StandardUnit.BYTES, 100.0))
  }

  test("7.decimal byte units pass through unchanged") {
    assert(
      convert(information.Kilobytes.symbol, information.Information.name, 2.0) == (
        StandardUnit.KILOBYTES,
        2.0))
    assert(
      convert(information.Megabytes.symbol, information.Information.name, 2.0) == (
        StandardUnit.MEGABYTES,
        2.0))
  }

  test("8.decimal byte units Gigabytes/Terabytes pass through unchanged") {
    assert(
      convert(information.Gigabytes.symbol, information.Information.name, 2.0) == (
        StandardUnit.GIGABYTES,
        2.0))
    assert(
      convert(information.Terabytes.symbol, information.Information.name, 2.0) == (
        StandardUnit.TERABYTES,
        2.0))
  }

  test("9.binary byte units rescale to their decimal CloudWatch counterpart") {
    // 1 KiB = 1.024 KB, 1 MiB = 1.048576 MB, 1 GiB = 1.073741824 GB, 1 TiB = 1.099511627776 TB
    assert(
      convert(information.Kibibytes.symbol, information.Information.name, 1.0) == (
        StandardUnit.KILOBYTES,
        information.Kibibytes(1.0).to(information.Kilobytes)))
    assert(
      convert(information.Mebibytes.symbol, information.Information.name, 1.0) == (
        StandardUnit.MEGABYTES,
        information.Mebibytes(1.0).to(information.Megabytes)))
    assert(
      convert(information.Gibibytes.symbol, information.Information.name, 1.0) == (
        StandardUnit.GIGABYTES,
        information.Gibibytes(1.0).to(information.Gigabytes)))
    assert(
      convert(information.Tebibytes.symbol, information.Information.name, 1.0) == (
        StandardUnit.TERABYTES,
        information.Tebibytes(1.0).to(information.Terabytes)))
  }

  // ---- information bits: decimal pass through, binary rescale --------------------------------------

  test("10.Bits and decimal bit units pass through unchanged") {
    assert(convert(information.Bits.symbol, information.Information.name, 8.0) == (StandardUnit.BITS, 8.0))
    assert(
      convert(information.Kilobits.symbol, information.Information.name, 2.0) == (StandardUnit.KILOBITS, 2.0))
    assert(
      convert(information.Megabits.symbol, information.Information.name, 2.0) == (StandardUnit.MEGABITS, 2.0))
    assert(
      convert(information.Gigabits.symbol, information.Information.name, 2.0) == (StandardUnit.GIGABITS, 2.0))
    assert(
      convert(information.Terabits.symbol, information.Information.name, 2.0) == (StandardUnit.TERABITS, 2.0))
  }

  test("11.binary bit units rescale to their decimal CloudWatch counterpart") {
    assert(
      convert(information.Kibibits.symbol, information.Information.name, 1.0) == (
        StandardUnit.KILOBITS,
        information.Kibibits(1.0).to(information.Kilobits)))
    assert(
      convert(information.Mebibits.symbol, information.Information.name, 1.0) == (
        StandardUnit.MEGABITS,
        information.Mebibits(1.0).to(information.Megabits)))
    assert(
      convert(information.Gibibits.symbol, information.Information.name, 1.0) == (
        StandardUnit.GIGABITS,
        information.Gibibits(1.0).to(information.Gigabits)))
    assert(
      convert(information.Tebibits.symbol, information.Information.name, 1.0) == (
        StandardUnit.TERABITS,
        information.Tebibits(1.0).to(information.Terabits)))
  }

  // ---- data-rate bytes/second: decimal pass through, binary rescale --------------------------------

  test("12.decimal byte-rate units pass through unchanged") {
    assert(
      convert(information.BytesPerSecond.symbol, information.DataRate.name, 100.0) ==
        (StandardUnit.BYTES_SECOND, 100.0))
    assert(
      convert(information.KilobytesPerSecond.symbol, information.DataRate.name, 2.0) ==
        (StandardUnit.KILOBYTES_SECOND, 2.0))
    assert(
      convert(information.MegabytesPerSecond.symbol, information.DataRate.name, 2.0) ==
        (StandardUnit.MEGABYTES_SECOND, 2.0))
    assert(
      convert(information.GigabytesPerSecond.symbol, information.DataRate.name, 2.0) ==
        (StandardUnit.GIGABYTES_SECOND, 2.0))
    assert(
      convert(information.TerabytesPerSecond.symbol, information.DataRate.name, 2.0) ==
        (StandardUnit.TERABYTES_SECOND, 2.0))
  }

  test("13.binary byte-rate units rescale to their decimal CloudWatch counterpart") {
    assert(convert(information.KibibytesPerSecond.symbol, information.DataRate.name, 1.0) ==
      (StandardUnit.KILOBYTES_SECOND, information.KibibytesPerSecond(1.0).to(information.KilobytesPerSecond)))
    assert(convert(information.MebibytesPerSecond.symbol, information.DataRate.name, 1.0) ==
      (StandardUnit.MEGABYTES_SECOND, information.MebibytesPerSecond(1.0).to(information.MegabytesPerSecond)))
    assert(convert(information.GibibytesPerSecond.symbol, information.DataRate.name, 1.0) ==
      (StandardUnit.GIGABYTES_SECOND, information.GibibytesPerSecond(1.0).to(information.GigabytesPerSecond)))
    assert(convert(information.TebibytesPerSecond.symbol, information.DataRate.name, 1.0) ==
      (StandardUnit.TERABYTES_SECOND, information.TebibytesPerSecond(1.0).to(information.TerabytesPerSecond)))
  }

  // ---- data-rate bits/second: decimal pass through, binary rescale ---------------------------------

  test("14.decimal bit-rate units pass through unchanged") {
    assert(
      convert(information.BitsPerSecond.symbol, information.DataRate.name, 100.0) ==
        (StandardUnit.BITS_SECOND, 100.0))
    assert(
      convert(information.KilobitsPerSecond.symbol, information.DataRate.name, 2.0) ==
        (StandardUnit.KILOBITS_SECOND, 2.0))
    assert(
      convert(information.MegabitsPerSecond.symbol, information.DataRate.name, 2.0) ==
        (StandardUnit.MEGABITS_SECOND, 2.0))
    assert(
      convert(information.GigabitsPerSecond.symbol, information.DataRate.name, 2.0) ==
        (StandardUnit.GIGABITS_SECOND, 2.0))
    assert(
      convert(information.TerabitsPerSecond.symbol, information.DataRate.name, 2.0) ==
        (StandardUnit.TERABITS_SECOND, 2.0))
  }

  test("15.binary bit-rate units rescale to their decimal CloudWatch counterpart") {
    assert(
      convert(information.KibibitsPerSecond.symbol, information.DataRate.name, 1.0) ==
        (StandardUnit.KILOBITS_SECOND, information.KibibitsPerSecond(1.0).to(information.KilobitsPerSecond)))
    assert(
      convert(information.MebibitsPerSecond.symbol, information.DataRate.name, 1.0) ==
        (StandardUnit.MEGABITS_SECOND, information.MebibitsPerSecond(1.0).to(information.MegabitsPerSecond)))
    assert(
      convert(information.GibibitsPerSecond.symbol, information.DataRate.name, 1.0) ==
        (StandardUnit.GIGABITS_SECOND, information.GibibitsPerSecond(1.0).to(information.GigabitsPerSecond)))
    assert(
      convert(information.TebibitsPerSecond.symbol, information.DataRate.name, 1.0) ==
        (StandardUnit.TERABITS_SECOND, information.TebibitsPerSecond(1.0).to(information.TerabitsPerSecond)))
  }

  // ---- fallthrough ---------------------------------------------------------------------------------

  test("16.an unknown unit symbol falls through to NONE, value unchanged") {
    assert(convert("weird", "unknown", 7.0) == (StandardUnit.NONE, 7.0))
  }

  test("17.a known symbol paired with the wrong dimension falls through to NONE") {
    // `ea` only matches under Dimensionless; under Time the guard fails and it becomes NONE
    assert(convert(Each.symbol, time.Time.name, 5.0) == (StandardUnit.NONE, 5.0))
    // `B` only matches under Information; under DataRate it falls through
    assert(convert(information.Bytes.symbol, information.DataRate.name, 100.0) == (StandardUnit.NONE, 100.0))
  }
}
