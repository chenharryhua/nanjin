package mtest

import java.time.{LocalDate, LocalTime, ZoneId, ZonedDateTime}

import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJDateTimeRange, NJTimestamp}
import org.scalatest.funsuite.AnyFunSuite

class DateTimeParserTest extends AnyFunSuite {
  val zoneId: ZoneId         = sydneyTime
  val range: NJDateTimeRange = NJDateTimeRange(sydneyTime)
  test("Local Date") {
    assert(
      range.withStartTime("2020-01-01").startTimestamp.get === NJTimestamp(
        ZonedDateTime.of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, zoneId)))
  }

  test("Local Time") {
    assert(
      range.withStartTime("00:00:00").zonedStartTime.get === ZonedDateTime
        .of(LocalDate.now, LocalTime.MIDNIGHT, zoneId))

  }

  test("Local Date Time") {
    assert(
      range.withStartTime("2020-01-01T00:00:00").zonedStartTime.get === ZonedDateTime
        .of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, zoneId))
  }

  test("UTC") {
    val date =
      NJTimestamp(
        ZonedDateTime.of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, ZoneId.of("Etc/UTC")))
    assert(range.withStartTime("2020-01-01T00:00:00Z").startTimestamp.get === date)
    assert(NJTimestamp("2020-01-01T00:00:00Z") === date)
    assert(NJTimestamp("2020-01-01T11:00+11:00") === date)
    assert(NJTimestamp("2020-01-01T11:00+11:00[Australia/Sydney]") === date)
  }

  test("Zoned Date Time") {
    val date =
      ZonedDateTime.of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, zoneId)
    assert(
      range
        .withStartTime("2020-01-01T00:00+11:00[Australia/Melbourne]")
        .zonedStartTime
        .get === date)

    assert(NJTimestamp("2020-01-01T00:00+11:00[Australia/Melbourne]").atZone(zoneId) === date)
  }
  test("Offset Date Time") {
    val date =
      ZonedDateTime.of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, zoneId)
    assert(range.withStartTime("2020-01-01T00:00+11:00").zonedStartTime.get === date)
    assert(NJTimestamp("2020-01-01T00:00+11:00").atZone(zoneId) === date)
  }
}
