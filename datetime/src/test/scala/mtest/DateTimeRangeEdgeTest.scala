package mtest

import cats.PartialOrder
import com.github.chenharryhua.nanjin.datetime.*
import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.FunSuite

import java.sql.Timestamp
import java.time.*
import java.util.UUID

class DateTimeRangeEdgeTest extends FunSuite {

  private val ZONE: ZoneId = ZoneOffset.UTC
  private val START: Instant = Instant.parse("2026-01-02T03:04:05Z")
  private val END: Instant = Instant.parse("2026-01-03T04:05:06Z")

  test("1.toString renders every bounded and unbounded shape") {
    val empty = DateTimeRange(ZONE)
    val start_only = empty.withStartTime(START)
    val end_only = empty.withEndTime(END)
    val bounded = start_only.withEndTime(END)

    assertEquals(empty.toString, "zoneId: Z, start: null, end: null, range: infinite")
    assertEquals(start_only.toString, "zoneId: Z, start: 2026-01-02T03:04:05, end: null, range: infinite")
    assertEquals(end_only.toString, "zoneId: Z, start: null, end: 2026-01-03T04:05:06, range: infinite")
    assertEquals(
      bounded.toString,
      "zoneId: Z, start: 2026-01-02T03:04:05, end: 2026-01-03T04:05:06, range: PT25H1M1S")
  }

  test("2.partial ordering covers every unbounded endpoint combination") {
    val order = summon[PartialOrder[DateTimeRange]]
    val unbounded = DateTimeRange(ZONE)
    val start_only = DateTimeRange(ZONE).withStartTime(START)
    val end_only = DateTimeRange(ZONE).withEndTime(END)
    val bounded = DateTimeRange(ZONE).withStartTime(START).withEndTime(END)
    val crossing = DateTimeRange(ZONE)
      .withStartTime(START.minusSeconds(1L))
      .withEndTime(END.minusSeconds(1L))

    assertEquals(order.partialCompare(unbounded, unbounded), 0.0)
    assertEquals(order.partialCompare(unbounded, start_only), 1.0)
    assertEquals(order.partialCompare(unbounded, end_only), 1.0)
    assertEquals(order.partialCompare(unbounded, bounded), 1.0)
    assertEquals(order.partialCompare(bounded, unbounded), -1.0)
    assert(java.lang.Double.isNaN(order.partialCompare(start_only, end_only)))
    assert(java.lang.Double.isNaN(order.partialCompare(bounded, crossing)))
  }

  test("3.withOneDay handles LocalDate.MAX without overflowing plusDays") {
    val range = DateTimeRange(ZONE).withOneDay(LocalDate.MAX)

    assertEquals(range.zonedStartTime.map(_.toLocalDate), Some(LocalDate.MAX))
    assertEquals(range.zonedStartTime.map(_.toLocalTime), Some(LocalTime.MIDNIGHT))
    assertEquals(range.zonedEndTime.map(_.toLocalDateTime), Some(LocalDateTime.MAX))
  }

  test("4.Tick constructor uses commence, conclude, and zone") {
    val tick = com.github.chenharryhua.nanjin.common.chrono.Tick(
      sequenceId = UUID.fromString("12345678-90ab-cdef-1234-567890abcdef"),
      launchTime = START.minusSeconds(10L),
      zoneId = ZoneId.of("Australia/Sydney"),
      index = 3L,
      commence = START,
      acquires = START.plusSeconds(1L),
      conclude = END
    )
    val range = DateTimeRange(tick)

    assertEquals(range.start, Some(START))
    assertEquals(range.end, Some(END))
    assertEquals(range.zoneId, tick.zoneId)
  }

  test("5.start and end overloads resolve to equivalent instants") {
    val local_date = LocalDate.of(2026, 1, 2)
    val local_time = LocalTime.of(3, 4, 5)
    val local_date_time = LocalDateTime.of(local_date, local_time)
    val zoned = local_date_time.atZone(ZONE)
    val offset = local_date_time.atOffset(ZoneOffset.UTC)
    val timestamp = Timestamp.from(START)

    assertEquals(DateTimeRange(ZONE).withStartTime(local_date_time).start, Some(START))
    assertEquals(DateTimeRange(ZONE).withStartTime(zoned).start, Some(START))
    assertEquals(DateTimeRange(ZONE).withStartTime(offset).start, Some(START))
    assertEquals(DateTimeRange(ZONE).withStartTime(timestamp).start, Some(START))
    assertEquals(DateTimeRange(ZONE).withStartTime(START.toEpochMilli).start, Some(START))
    assertEquals(DateTimeRange(ZONE).withEndTime(local_date_time).end, Some(START))
    assertEquals(DateTimeRange(ZONE).withEndTime(zoned).end, Some(START))
    assertEquals(DateTimeRange(ZONE).withEndTime(offset).end, Some(START))
    assertEquals(DateTimeRange(ZONE).withEndTime(timestamp).end, Some(START))
    assertEquals(DateTimeRange(ZONE).withEndTime(START.toEpochMilli).end, Some(START))
    assertEquals(
      DateTimeRange(ZONE).withStartTime(local_date).start,
      Some(local_date.atStartOfDay(ZONE).toInstant))
  }

  test("6.string setters accept each supported date-time representation") {
    val expected_local = LocalDateTime.of(2026, 1, 2, 3, 4, 5).toInstant(ZoneOffset.UTC)
    val inputs = List(
      "2026-01-02T03:04:05Z" -> expected_local,
      "2026-01-02T03:04:05+00:00" -> expected_local,
      "2026-01-02T03:04:05Z[UTC]" -> expected_local,
      "2026-01-02T03:04:05" -> expected_local,
      "2026-01-02" -> LocalDate.of(2026, 1, 2).atStartOfDay(ZONE).toInstant
    )

    inputs.foreach { case (input, expected) =>
      assertEquals(DateTimeRange(ZONE).withStartTime(input).start, Some(expected))
      assertEquals(DateTimeRange(ZONE).withEndTime(input).end, Some(expected))
    }
  }

  test("7.JSON decoder preserves one-sided bounds") {
    val start_only = Json.obj(
      "zone_id" -> ZONE.asJson,
      "start" -> LocalDateTime.ofInstant(START, ZONE).asJson,
      "end" -> Json.Null)
    val end_only = Json.obj(
      "zone_id" -> ZONE.asJson,
      "start" -> Json.Null,
      "end" -> LocalDateTime.ofInstant(END, ZONE).asJson)

    assertEquals(start_only.as[DateTimeRange].toOption, Some(DateTimeRange(ZONE).withStartTime(START)))
    assertEquals(end_only.as[DateTimeRange].toOption, Some(DateTimeRange(ZONE).withEndTime(END)))
  }

  test("8.negative and zero-width ranges retain signed duration semantics") {
    val negative = DateTimeRange(ZONE).withStartTime(END).withEndTime(START)
    val zero = DateTimeRange(ZONE).withStartTime(START).withEndTime(START)

    assertEquals(negative.javaDuration, Some(Duration.between(END, START)))
    assert(negative.days.isEmpty)
    assertEquals(zero.javaDuration, Some(Duration.ZERO))
    assertEquals(zero.days.toList, Nil)
  }
}
