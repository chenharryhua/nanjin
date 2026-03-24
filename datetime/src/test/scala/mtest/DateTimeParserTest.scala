package mtest

import cats.Alternative
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.datetime.{DateTimeParser, DateTimeRange}
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDate, LocalTime, ZoneId, ZonedDateTime}

class DateTimeParserTest extends AnyFunSuite {

  val zoneId: ZoneId = sydneyTime
  val range: DateTimeRange = DateTimeRange(sydneyTime)

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

  test("Zoned Date Time") {
    val date =
      ZonedDateTime.of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, zoneId)
    assert(range.withStartTime("2020-01-01T00:00+11:00[Australia/Melbourne]").zonedStartTime.get === date)

  }
  test("Offset Date Time") {
    val date =
      ZonedDateTime.of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, zoneId)
    assert(range.withStartTime("2020-01-01T00:00+11:00").zonedStartTime.get === date)
  }

  test("empty parser always return failure") {
    assert(Alternative[DateTimeParser].empty.parse("2020-01-01T11:00+11:00[Australia/Sydney]").isLeft)
  }
}
