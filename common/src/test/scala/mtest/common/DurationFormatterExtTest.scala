package mtest.common

import com.github.chenharryhua.nanjin.common.DurationFormatter
import org.scalatest.funsuite.AnyFunSuite
import squants.time.Milliseconds

import java.time.{Duration as JavaDuration, Instant, ZoneOffset, ZonedDateTime}
import scala.concurrent.duration.*

class DurationFormatterExtTest extends AnyFunSuite {

  // --- create(maxParts) ---

  test("1.create(1) - shows only the largest unit") {
    val fmt = DurationFormatter.create(1)
    val duration = JavaDuration.ofMinutes(67)
    assert(fmt.format(duration) == "1 hour")
  }

  test("2.create(3) - shows up to three units") {
    val fmt = DurationFormatter.create(3)
    // 1 hour + 2 minutes + 3 seconds + 456 millis
    val duration = JavaDuration.ofSeconds(3723).plusMillis(456)
    assert(fmt.format(duration) == "1 hour 2 minutes 3 seconds")
  }

  test("3.create(7) - shows all available units") {
    val fmt = DurationFormatter.create(7)
    // 1 day + 2 hours + 3 minutes + 4 seconds + 5 millis + 6 micros + 7 nanos
    val nanos =
      1.day.toNanos + 2.hours.toNanos + 3.minutes.toNanos + 4.seconds.toNanos +
        5.millis.toNanos + 6.micros.toNanos + 7L
    val duration = JavaDuration.ofNanos(nanos)
    assert(fmt.format(duration) == "1 day 2 hours 3 minutes 4 seconds 5 millis 6 micros 7 nanos")
  }

  // --- zero duration ---

  test("4.zero duration formats as '0 second'") {
    val fmt = DurationFormatter.defaultFormatter
    assert(fmt.format(JavaDuration.ZERO) == "0 second")
  }

  test("5.create(1) zero duration") {
    val fmt = DurationFormatter.create(1)
    assert(fmt.format(JavaDuration.ZERO) == "0 second")
  }

  // --- negative durations ---

  test("6.negative duration - prepends minus sign") {
    val fmt = DurationFormatter.defaultFormatter
    val duration = JavaDuration.ofSeconds(-65)
    assert(fmt.format(duration) == "-1 minute 5 seconds")
  }

  test("7.negative duration - single unit") {
    val fmt = DurationFormatter.create(1)
    val duration = JavaDuration.ofHours(-3)
    assert(fmt.format(duration) == "-3 hours")
  }

  // --- format(Instant, Instant) ---

  test("8.format(Instant, Instant) computes duration between two instants") {
    val fmt = DurationFormatter.defaultFormatter
    val start = Instant.parse("2024-01-01T00:00:00Z")
    val end = Instant.parse("2024-01-01T01:30:00Z")
    assert(fmt.format(start, end) == "1 hour 30 minutes")
  }

  test("9.format(Instant, Instant) - reversed instants give negative") {
    val fmt = DurationFormatter.defaultFormatter
    val start = Instant.parse("2024-01-01T01:00:00Z")
    val end = Instant.parse("2024-01-01T00:00:00Z")
    assert(fmt.format(start, end) == "-1 hour")
  }

  // --- format(ZonedDateTime, ZonedDateTime) ---

  test("10.format(ZonedDateTime, ZonedDateTime) computes duration correctly") {
    val fmt = DurationFormatter.defaultFormatter
    val start = ZonedDateTime.of(2024, 3, 1, 10, 0, 0, 0, ZoneOffset.UTC)
    val end = ZonedDateTime.of(2024, 3, 1, 12, 45, 0, 0, ZoneOffset.UTC)
    assert(fmt.format(start, end) == "2 hours 45 minutes")
  }

  // --- format(Time) using squants ---

  test("11.format(squants Time) converts correctly") {
    val fmt = DurationFormatter.defaultFormatter
    val time = Milliseconds(3500)
    assert(fmt.format(time) == "3 seconds 500 millis")
  }

  // --- plural edge cases ---

  test("12.exactly 1 day shows singular") {
    val fmt = DurationFormatter.create(1)
    assert(fmt.format(JavaDuration.ofDays(1)) == "1 day")
  }

  test("13.multiple days shows plural") {
    val fmt = DurationFormatter.create(1)
    assert(fmt.format(JavaDuration.ofDays(5)) == "5 days")
  }
}
