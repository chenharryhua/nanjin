package mtest.common

import com.github.chenharryhua.nanjin.common.DurationFormatter
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration

class DurationFormatterTest extends AnyFunSuite {
  private val fmt = DurationFormatter.defaultFormatter

  test("1.nano") {
    val duration = Duration.ofNanos(500)
    assert(fmt.format(duration) == "500 nanos")
  }

  test("2.micro") {
    val duration = Duration.ofNanos(1500)
    assert(fmt.format(duration) == "1 micro 500 nanos")
  }
  test("3.micro 2") {
    val duration = Duration.ofNanos(1000)
    assert(fmt.format(duration) == "1 micro")
  }
  test("4.micro 3") {
    val duration = Duration.ofNanos(2000)
    assert(fmt.format(duration) == "2 micros")
  }

  test("5.milli") {
    val duration = Duration.ofNanos(1_500_001)
    assert(fmt.format(duration) == "1 milli 500 micros")
  }
  test("6.milli 2") {
    val duration = Duration.ofNanos(1_000_001)
    assert(fmt.format(duration) == "1 milli 1 nano")
  }

  test("7.second") {
    val duration = Duration.ofNanos(1_500_100_000)
    assert(fmt.format(duration) == "1 second 500 millis")
  }

  test("8.second 2") {
    val duration = Duration.ofNanos(1_000_100_000)
    assert(fmt.format(duration) == "1 second 100 micros")
  }

  test("9.second 3") {
    val duration = Duration.ofNanos(2_500_100_000L)
    assert(fmt.format(duration) == "2 seconds 500 millis")
  }

  test("10.minute") {
    val duration = Duration.ofMillis(65_900)
    assert(fmt.format(duration) == "1 minute 5 seconds")
  }
  test("11.minute 2") {
    val duration = Duration.ofSeconds(120)
    assert(fmt.format(duration) == "2 minutes")
  }
  test("12.minute 3") {
    val duration = Duration.ofSeconds(60)
    assert(fmt.format(duration) == "1 minute")
  }

  test("13.hour") {
    val duration = Duration.ofMinutes(67)
    assert(fmt.format(duration) == "1 hour 7 minutes")
  }

  test("14.hour 2") {
    val duration = Duration.ofSeconds(3659)
    assert(fmt.format(duration) == "1 hour 59 seconds")
  }
}
