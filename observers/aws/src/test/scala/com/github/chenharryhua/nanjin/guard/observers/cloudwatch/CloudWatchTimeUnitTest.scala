package com.github.chenharryhua.nanjin.guard.observers.cloudwatch

import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit
import squants.{information, time, Dimensionless, Dozen, Each, Gross, Percent, Score}

/** Tests for `CloudWatchTimeUnit.toStandardUnit`, the pure mapping from a squants unit symbol + dimension
  * name to a CloudWatch `StandardUnit` and a (possibly rescaled) value.
  *
  * The mapping only fires when both the unit symbol AND the dimension name match; a symbol paired with the
  * wrong dimension falls through to `NONE`. Binary units (Ki/Mi/...) are rescaled to their decimal CloudWatch
  * counterparts, and nanoseconds are folded into microseconds.
  */
class CloudWatchTimeUnitTest extends AnyFunSuite {

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

  test("8.binary byte units rescale to their decimal CloudWatch counterpart") {
    // 1 KiB = 1.024 KB, 1 MiB = 1.048576 MB
    assert(
      convert(information.Kibibytes.symbol, information.Information.name, 1.0) == (
        StandardUnit.KILOBYTES,
        1.024))
    assert(
      convert(information.Mebibytes.symbol, information.Information.name, 1.0) == (
        StandardUnit.MEGABYTES,
        1.048576))
  }

  test("9.data-rate: decimal passes through, binary rescales") {
    assert(
      convert(information.BytesPerSecond.symbol, information.DataRate.name, 100.0) ==
        (StandardUnit.BYTES_SECOND, 100.0))
    assert(
      convert(information.KibibytesPerSecond.symbol, information.DataRate.name, 1.0) ==
        (StandardUnit.KILOBYTES_SECOND, 1.024))
  }

  // ---- fallthrough ---------------------------------------------------------------------------------

  test("10.an unknown unit symbol falls through to NONE, value unchanged") {
    assert(convert("weird", "unknown", 7.0) == (StandardUnit.NONE, 7.0))
  }

  test("11.a known symbol paired with the wrong dimension falls through to NONE") {
    // `ea` only matches under Dimensionless; under Time the guard fails and it becomes NONE
    assert(convert(Each.symbol, time.Time.name, 5.0) == (StandardUnit.NONE, 5.0))
    // `B` only matches under Information; under DataRate it falls through
    assert(convert(information.Bytes.symbol, information.DataRate.name, 100.0) == (StandardUnit.NONE, 100.0))
  }
}
