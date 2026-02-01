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

  test("empty DateTimeRange is infinite") {
    val dtr = DateTimeRange(utc)

    assert(dtr.startTimestamp.isEmpty)
    assert(dtr.endTimestamp.isEmpty)
    assert(dtr.inBetween(0L))
  }

  test("withStartTime only") {
    val start = LocalDateTime.parse("2024-01-01T10:00:00")
    val dtr = DateTimeRange(utc).withStartTime(start)

    assert(dtr.startTimestamp.nonEmpty)
    assert(dtr.endTimestamp.isEmpty)
    assert(dtr.zonedStartTime.get.toLocalDateTime == start)
  }

  test("withEndTime only") {
    val end = LocalDateTime.parse("2024-01-01T18:00:00")
    val dtr = DateTimeRange(utc).withEndTime(end)

    assert(dtr.startTimestamp.isEmpty)
    assert(dtr.endTimestamp.nonEmpty)
    assert(dtr.zonedEndTime.get.toLocalDateTime == end)
  }

  test("withStartTime and withEndTime") {
    val start = LocalDateTime.parse("2024-01-01T10:00:00")
    val end = LocalDateTime.parse("2024-01-01T18:00:00")

    val dtr =
      DateTimeRange(utc).withStartTime(start).withEndTime(end)

    assert(dtr.duration.contains(8.hours))
  }

  // ------------------------------------------------------------
  // Day resolution
  // ------------------------------------------------------------

  test("days returns inclusive list") {
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

  test("days is empty for infinite ranges") {
    assert(DateTimeRange(utc).days.isEmpty)
  }

  // ------------------------------------------------------------
  // Subranges
  // ------------------------------------------------------------

  test("subranges splits range correctly") {
    val dtr =
      DateTimeRange(utc)
        .withStartTime(LocalDateTime.parse("2024-01-01T00:00:00"))
        .withEndTime(LocalDateTime.parse("2024-01-01T03:00:00"))

    val subs = dtr.subranges(1.hour)

    assert(subs.size == 3)
    assert(subs.forall(_.duration.contains(1.hour)))
  }

  test("subranges empty for infinite range") {
    assert(DateTimeRange(utc).subranges(1.hour).isEmpty)
  }

  // ------------------------------------------------------------
  // inBetween semantics
  // ------------------------------------------------------------

  test("inBetween respects half-open interval") {
    val start = LocalDateTime.parse("2024-01-01T00:00:00")
    val end = LocalDateTime.parse("2024-01-01T01:00:00")

    val dtr =
      DateTimeRange(utc).withStartTime(start).withEndTime(end)

    val s = dtr.startTimestamp.get.milliseconds
    val e = dtr.endTimestamp.get.milliseconds

    assert(dtr.inBetween(s))
    assert(dtr.inBetween(s + 1))
    assert(!dtr.inBetween(e))
  }

  // ------------------------------------------------------------
  // Period / Duration
  // ------------------------------------------------------------

  test("period computes date-based period") {
    val dtr =
      DateTimeRange(utc)
        .withStartTime(LocalDate.parse("2024-01-01"))
        .withEndTime(LocalDate.parse("2024-01-10"))

    assert(dtr.period.contains(Period.ofDays(9)))
  }

  test("javaDuration computes time-based duration") {
    val dtr =
      DateTimeRange(utc)
        .withStartTime(LocalDateTime.parse("2024-01-01T00:00:00"))
        .withEndTime(LocalDateTime.parse("2024-01-01T00:30:00"))

    assert(dtr.javaDuration.contains(java.time.Duration.ofMinutes(30)))
  }

  // ------------------------------------------------------------
  // PartialOrder
  // ------------------------------------------------------------

  test("partial order: containing range is greater") {
    val outer =
      DateTimeRange(utc).withStartTime("2024-01-01T00:00:00").withEndTime("2024-01-02T00:00:00")

    val inner =
      DateTimeRange(utc).withStartTime("2024-01-01T06:00:00").withEndTime("2024-01-01T12:00:00")

    val po = implicitly[cats.PartialOrder[DateTimeRange]]

    assert(po.partialCompare(outer, inner) == 1.0)
    assert(po.partialCompare(inner, outer) == -1.0)
  }

  test("partial order: overlapping but non-containing is NaN") {
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

  test("circe decoder: optional start/end") {
    val json =
      """
        |{
        |  "zone_id": "UTC",
        |  "start": "2024-01-01T10:00:00",
        |  "end": null
        |}
        |""".stripMargin

    val dtr = decode[DateTimeRange](json).toOption.get

    assert(dtr.startTimestamp.nonEmpty)
    assert(dtr.endTimestamp.isEmpty)
  }

  test("circe round-trip preserves semantics") {
    val original =
      DateTimeRange(utc)
        .withStartTime(LocalDateTime.parse("2024-01-01T10:00:00"))
        .withEndTime(LocalDateTime.parse("2024-01-01T18:00:00"))

    val decoded =
      decode[DateTimeRange](original.asJson.noSpaces).toOption.get

    assert(decoded.startTimestamp == original.startTimestamp)
    assert(decoded.endTimestamp == original.endTimestamp)
    assert(decoded.zoneId == original.zoneId)
  }
}
