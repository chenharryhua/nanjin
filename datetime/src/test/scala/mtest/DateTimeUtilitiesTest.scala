package mtest

import cats.{Hash, Order, Show}
import com.github.chenharryhua.nanjin.datetime.*
import com.github.chenharryhua.nanjin.datetime.instances.given
import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.FunSuite

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import scala.concurrent.duration.*

class DateTimeUtilitiesTest extends FunSuite {

  test("1.SQL Timestamp Hash, Eq, Order, and Show use timestamp value semantics") {
    val instant = Instant.parse("2026-01-02T03:04:05.123456789Z")
    val first = Timestamp.from(instant)
    val equal = Timestamp.from(instant)
    val later = Timestamp.from(instant.plusNanos(1L))
    val hash = summon[Hash[Timestamp]]
    val order = summon[Order[Timestamp]]
    val show = summon[Show[Timestamp]]

    assert(hash.eqv(first, equal))
    assertEquals(hash.hash(first), hash.hash(equal))
    assert(order.lt(first, later))
    assert(order.gt(later, first))
    assertEquals(show.show(first), first.toString)
  }

  test("2.SQL Date Hash, Eq, Order, and Show use date value semantics") {
    val first = Date.valueOf(LocalDate.of(2026, 1, 2))
    val equal = Date.valueOf(LocalDate.of(2026, 1, 2))
    val later = Date.valueOf(LocalDate.of(2026, 1, 3))
    val hash = summon[Hash[Date]]
    val order = summon[Order[Date]]
    val show = summon[Show[Date]]

    assert(hash.eqv(first, equal))
    assertEquals(hash.hash(first), hash.hash(equal))
    assert(order.lt(first, later))
    assert(order.gt(later, first))
    assertEquals(show.show(first), "2026-01-02")
  }

  test("3.SQL Timestamp and Date JSON codecs round-trip without losing supported precision") {
    val timestamp = Timestamp.from(Instant.parse("2026-01-02T03:04:05.123456789Z"))
    val date = Date.valueOf("2026-01-02")

    assertEquals(timestamp.asJson.as[Timestamp].toOption.map(_.toInstant), Some(timestamp.toInstant))
    assertEquals(date.asJson.as[Date].toOption.map(_.toLocalDate), Some(date.toLocalDate))
    assertEquals(timestamp.asJson, Json.fromString("2026-01-02T03:04:05.123456789Z"))
    assertEquals(date.asJson, Json.fromString("2026-01-02"))
  }

  test("4.FiniteDuration JSON codec round-trips positive, zero, and negative durations") {
    List(1500.millis, Duration.Zero, (-2).seconds).foreach { duration =>
      assertEquals(duration.asJson.as[FiniteDuration].toOption, Some(duration))
    }
  }

  test("5.toLocalDateTime anchors LocalDate at midnight and LocalTime on today") {
    val date = LocalDate.of(2026, 1, 2)
    val time = LocalTime.of(3, 4, 5, 6)
    val before = LocalDate.now()
    val from_time = toLocalDateTime(time)
    val after = LocalDate.now()

    assertEquals(toLocalDateTime(date), LocalDateTime.of(date, LocalTime.MIDNIGHT))
    assertEquals(from_time.toLocalTime, time)
    assert(Set(before, after).contains(from_time.toLocalDate))
  }

  test("6.resolution helpers truncate only lower-order fields") {
    val value = LocalDateTime.of(2026, 1, 2, 3, 4, 5, 678_000_000)

    assertEquals(dayResolution(value), LocalDate.of(2026, 1, 2))
    assertEquals(hourResolution(value), LocalDateTime.of(2026, 1, 2, 3, 0, 0))
    assertEquals(minuteResolution(value), LocalDateTime.of(2026, 1, 2, 3, 4, 0))
    assertEquals(secondResolution(value), LocalDateTime.of(2026, 1, 2, 3, 4, 5))
  }

  test("7.duration constants carry their documented units") {
    assertEquals(oneMillisecond, 1.millis)
    assertEquals(oneSecond, 1.second)
    assertEquals(oneMinute, 1.minute)
    assertEquals(oneHour, 1.hour)
    assertEquals(oneDay, 1.day)
  }

  test("8.LocalTime distance handles forward, midnight wrap, equality, and nanoseconds") {
    assertEquals(distance(LocalTime.of(1, 0), LocalTime.of(2, 0)), 1.hour)
    assertEquals(distance(LocalTime.of(23, 0), LocalTime.of(1, 0)), 2.hours)
    assertEquals(distance(LocalTime.NOON, LocalTime.NOON), Duration.Zero)
    assertEquals(distance(LocalTime.MIDNIGHT, LocalTime.MIDNIGHT.plusNanos(1L)), 1.nano)
  }
}
