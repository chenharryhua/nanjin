package mtest

import java.time.{LocalDate, LocalTime, ZoneId, ZonedDateTime}

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import org.scalatest.funsuite.AnyFunSuite

class NJTimestampTest extends AnyFunSuite {
  test("Local Date") {
    assert(
      NJTimestamp("2020-01-01").local === ZonedDateTime
        .of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, ZoneId.systemDefault()))
  }

  test("Local Time") {
    assert(
      NJTimestamp("00:00:00").local === ZonedDateTime
        .of(LocalDate.now, LocalTime.MIDNIGHT, ZoneId.systemDefault()))

  }

  test("Local Date Time") {
    assert(
      NJTimestamp("2020-01-01T00:00:00").local === ZonedDateTime
        .of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, ZoneId.systemDefault()))
  }

  test("UTC") {
    assert(
      NJTimestamp("2020-01-01T00:00:00Z").utc === ZonedDateTime
        .of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, ZoneId.of("Etc/UTC")))
  }

  test("Zoned Date Time") {
    assert(
      NJTimestamp("2020-01-01T00:00+11:00[Australia/Melbourne]").local === ZonedDateTime
        .of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, ZoneId.systemDefault()))
  }

}
