package mtest

import com.github.chenharryhua.nanjin.datetime.LocalTimeRange
import munit.FunSuite

import java.time.*
import scala.concurrent.duration.DurationInt

class LocalTimeRangeSpec extends FunSuite {

  test("LocalTimeRange.inBetween(LocalTime): return false for negative durations") {
    val ltr = LocalTimeRange(LocalTime.of(10, 0), Duration.ofHours(-1))
    assert(!ltr.inBetween(LocalTime.of(10, 0)))
    assert(!ltr.inBetween(LocalTime.of(9, 0)))
  }

  test("LocalTimeRange.inBetween(LocalTime): return true for duration >= 24 hours") {
    val ltr = LocalTimeRange(LocalTime.of(0, 0), Duration.ofHours(24))
    assert(ltr.inBetween(LocalTime.of(0, 0)))
    assert(ltr.inBetween(LocalTime.of(12, 0)))
    assert(ltr.inBetween(LocalTime.of(23, 59)))
  }

  test("LocalTimeRange.inBetween(LocalTime): handle normal non-cross-midnight durations correctly") {
    val ltr = LocalTimeRange(LocalTime.of(10, 0), Duration.ofHours(5)) // 10:00 -> 15:00
    assert(!ltr.inBetween(LocalTime.of(9, 0)))
    assert(ltr.inBetween(LocalTime.of(10, 0)))
    assert(ltr.inBetween(LocalTime.of(12, 0)))
    assert(!ltr.inBetween(LocalTime.of(15, 0)))
    assert(!ltr.inBetween(LocalTime.of(16, 0)))
  }

  test("LocalTimeRange.inBetween(LocalTime): handle cross-midnight durations correctly") {
    val ltr = LocalTimeRange(LocalTime.of(23, 0), Duration.ofHours(2)) // 23:00 -> 01:00
    assert(!ltr.inBetween(LocalTime.of(22, 59)))
    assert(ltr.inBetween(LocalTime.of(23, 0)))
    assert(ltr.inBetween(LocalTime.of(23, 30)))
    assert(ltr.inBetween(LocalTime.of(0, 0)))
    assert(ltr.inBetween(LocalTime.of(0, 59)))
    assert(!ltr.inBetween(LocalTime.of(1, 0)))
    assert(!ltr.inBetween(LocalTime.of(2, 0)))
  }

  test("LocalTimeRange.inBetween(LocalTime): handle exact zero duration") {
    val ltr = LocalTimeRange(LocalTime.of(12, 0), Duration.ZERO)
    assert(!ltr.inBetween(LocalTime.of(12, 0)))
    assert(!ltr.inBetween(LocalTime.of(12, 1)))
  }

  test("LocalTimeRange.inBetween(ZonedDateTime): delegate to LocalTime correctly") {
    val ltr = LocalTimeRange(LocalTime.of(22, 0), Duration.ofHours(4)) // 22:00 -> 02:00
    val zdt1 = ZonedDateTime.of(LocalDate.of(2026, 2, 1), LocalTime.of(23, 0), ZoneId.of("UTC"))
    val zdt2 = ZonedDateTime.of(LocalDate.of(2026, 2, 2), LocalTime.of(1, 30), ZoneId.of("UTC"))
    val zdt3 = ZonedDateTime.of(LocalDate.of(2026, 2, 2), LocalTime.of(2, 0), ZoneId.of("UTC"))

    assert(ltr.inBetween(zdt1))
    assert(ltr.inBetween(zdt2))
    assert(!ltr.inBetween(zdt3))
  }

  test("LocalTimeRange companion apply: convert FiniteDuration to Duration") {
    val ltr = LocalTimeRange(LocalTime.of(10, 0), 2.hours)
    assert(ltr.inBetween(LocalTime.of(11, 0)))
    assert(!ltr.inBetween(LocalTime.of(12, 0)))
  }
}
