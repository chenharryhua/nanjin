package mtest

import java.time.{LocalDate, LocalTime, ZoneId, ZonedDateTime}

import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import org.scalatest.funsuite.AnyFunSuite

class DateTimeParserTest extends AnyFunSuite {
  val range: NJDateTimeRange = NJDateTimeRange.infinite
  test("Local Date") {
    assert(
      range.withStartTime("2020-01-01").startTimestamp.get === NJTimestamp(
        ZonedDateTime.of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, ZoneId.systemDefault())))
  }

  test("Local Time") {
    assert(
      range.withStartTime("00:00:00").zonedStartTime.get === ZonedDateTime
        .of(LocalDate.now, LocalTime.MIDNIGHT, ZoneId.systemDefault()))

  }

  test("Local Date Time") {
    assert(
      range.withStartTime("2020-01-01T00:00:00").zonedStartTime.get === ZonedDateTime
        .of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, ZoneId.systemDefault()))
  }

  test("UTC") {
    assert(
      range.withStartTime("2020-01-01T00:00:00Z").startTimestamp.get === NJTimestamp(
        ZonedDateTime.of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, ZoneId.of("Etc/UTC"))))
  }

  test("Zoned Date Time") {
    assert(
      range
        .withStartTime("2020-01-01T00:00+11:00[Australia/Melbourne]")
        .zonedStartTime
        .get === ZonedDateTime
        .of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, ZoneId.systemDefault()))
  }

  test("Customized Date") {
    assert(
      range.withStartTime("20200101").zonedStartTime.get === ZonedDateTime
        .of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, ZoneId.systemDefault()))
  }
}
