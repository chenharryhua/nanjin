package mtest

import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import io.circe.jawn.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.*
import scala.concurrent.duration.*
final class DateTimeRangeSpec extends AnyFunSuite {

  private val utc: ZoneId = ZoneId.of("UTC")

  // ------------------------------------------------------------
  // Construction
  // ------------------------------------------------------------

  test("1.empty DateTimeRange is infinite") {
    val dtr = DateTimeRange(utc)

    assert(dtr.start.isEmpty)
    assert(dtr.end.isEmpty)
  }

  test("2.withStartTime only") {
    val start = LocalDateTime.parse("2024-01-01T10:00:00")
    val dtr = DateTimeRange(utc).withStartTime(start)

    assert(dtr.start.nonEmpty)
    assert(dtr.end.isEmpty)
    assert(dtr.zonedStartTime.get.toLocalDateTime == start)
  }

  test("3.withEndTime only") {
    val end = LocalDateTime.parse("2024-01-01T18:00:00")
    val dtr = DateTimeRange(utc).withEndTime(end)

    assert(dtr.start.isEmpty)
    assert(dtr.end.nonEmpty)
    assert(dtr.zonedEndTime.get.toLocalDateTime == end)
  }

  test("4.withStartTime and withEndTime") {
    val start = LocalDateTime.parse("2024-01-01T10:00:00")
    val end = LocalDateTime.parse("2024-01-01T18:00:00")

    val dtr =
      DateTimeRange(utc).withStartTime(start).withEndTime(end)

    assert(dtr.finiteDuration.contains(8.hours))
  }

  // ------------------------------------------------------------
  // Day resolution
  // ------------------------------------------------------------

  test("5.days returns inclusive list") {
    val dtr =
      DateTimeRange(utc)
        .withStartTime(LocalDate.parse("2024-01-01"))
        .withEndTime(LocalDate.parse("2024-01-03"))

    val days = dtr.days

    assert(
      days == List(
        LocalDate.parse("2024-01-01"),
        LocalDate.parse("2024-01-02"),
        LocalDate.parse("2024-01-03")
      ))
  }

  test("6.days is empty for infinite ranges") {
    assert(DateTimeRange(utc).days.isEmpty)
  }

  // ------------------------------------------------------------
  // Subranges
  // ------------------------------------------------------------

  test("7.subranges splits range correctly") {
    val dtr =
      DateTimeRange(utc)
        .withStartTime(LocalDateTime.parse("2024-01-01T00:00:00"))
        .withEndTime(LocalDateTime.parse("2024-01-01T03:00:00"))

    val subs = dtr.subranges(1.hour)

    assert(subs.size == 3)
    assert(subs.forall(_.finiteDuration.contains(1.hour)))
  }

  test("8.subranges empty for infinite range") {
    assert(DateTimeRange(utc).subranges(1.hour).isEmpty)
  }

  // ------------------------------------------------------------
  // inBetween semantics
  // ------------------------------------------------------------

  test("9.inBetween respects half-open interval") {
    val start = LocalDateTime.parse("2024-01-01T00:00:00")
    val end = LocalDateTime.parse("2024-01-01T01:00:00")

    val dtr =
      DateTimeRange(utc).withStartTime(start).withEndTime(end)

    val s = dtr.start.get
    val e = dtr.end.get

    assert(dtr.inBetween(s))
    assert(dtr.inBetween(s.plusSeconds(1)))
    assert(!dtr.inBetween(e))
  }

  // ------------------------------------------------------------
  // Period / Duration
  // ------------------------------------------------------------

  test("10.period computes date-based period") {
    val dtr =
      DateTimeRange(utc)
        .withStartTime(LocalDate.parse("2024-01-01"))
        .withEndTime(LocalDate.parse("2024-01-10"))

    assert(dtr.period.contains(Period.ofDays(9)))
  }

  test("11.javaDuration computes time-based duration") {
    val dtr =
      DateTimeRange(utc)
        .withStartTime(LocalDateTime.parse("2024-01-01T00:00:00"))
        .withEndTime(LocalDateTime.parse("2024-01-01T00:30:00"))

    assert(dtr.javaDuration.contains(java.time.Duration.ofMinutes(30)))
  }

  // ------------------------------------------------------------
  // PartialOrder
  // ------------------------------------------------------------

  test("12.partial order: containing range is greater") {
    val outer =
      DateTimeRange(utc).withStartTime("2024-01-01T00:00:00").withEndTime("2024-01-02T00:00:00")

    val inner =
      DateTimeRange(utc).withStartTime("2024-01-01T06:00:00").withEndTime("2024-01-01T12:00:00")

    val po = implicitly[cats.PartialOrder[DateTimeRange]]

    assert(po.partialCompare(outer, inner) == 1.0)
    assert(po.partialCompare(inner, outer) == -1.0)
  }

  test("13.partial order: overlapping but non-containing is NaN") {
    val a =
      DateTimeRange(utc).withStartTime("2024-01-01T00:00:00").withEndTime("2024-01-01T12:00:00")

    val b =
      DateTimeRange(utc).withStartTime("2024-01-01T06:00:00").withEndTime("2024-01-01T18:00:00")

    val po = implicitly[cats.PartialOrder[DateTimeRange]]

    assert(po.partialCompare(a, b).isNaN)
  }

  // ------------------------------------------------------------
  // Circe codec
  // ------------------------------------------------------------

  test("14.circe decoder: optional start/end") {
    val json =
      """
        |{
        |  "zone_id": "UTC",
        |  "start": "2024-01-01T10:00:00",
        |  "end": null
        |}
        |""".stripMargin

    val dtr = decode[DateTimeRange](json).toOption.get

    assert(dtr.start.nonEmpty)
    assert(dtr.end.isEmpty)
  }

  test("15.circe round-trip preserves semantics") {
    val original =
      DateTimeRange(utc)
        .withStartTime(LocalDateTime.parse("2024-01-01T10:00:00"))
        .withEndTime(LocalDateTime.parse("2024-01-01T18:00:00"))

    val decoded =
      decode[DateTimeRange](original.asJson.noSpaces).toOption.get

    assert(decoded.start == original.start)
    assert(decoded.end == original.end)
    assert(decoded.zoneId == original.zoneId)
  }
}
