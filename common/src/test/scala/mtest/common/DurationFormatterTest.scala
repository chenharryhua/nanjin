package mtest.common

import com.github.chenharryhua.nanjin.common.DurationFormatter
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration

class DurationFormatterTest extends AnyFunSuite {
  private val fmt = DurationFormatter.defaultFormatter

  test("nano") {
    val duration = Duration.ofNanos(500)
    assert(fmt.format(duration) == "500 nano")
  }

  test("micro") {
    val duration = Duration.ofNanos(1500)
    assert(fmt.format(duration) == "1 micro 500 nano")
  }
  test("micro 2") {
    val duration = Duration.ofNanos(1000)
    assert(fmt.format(duration) == "1 micro")
  }
  test("micro 3") {
    val duration = Duration.ofNanos(2000)
    assert(fmt.format(duration) == "2 micro")
  }

  test("milli") {
    val duration = Duration.ofNanos(1_500_001)
    assert(fmt.format(duration) == "1 milli 500 micro")
  }
  test("milli 2") {
    val duration = Duration.ofNanos(1_000_001)
    assert(fmt.format(duration) == "1 milli")
  }

  test("second") {
    val duration = Duration.ofNanos(1_500_100_000)
    assert(fmt.format(duration) == "1 second 500 milli")
  }

  test("second 2") {
    val duration = Duration.ofNanos(1_000_100_000)
    assert(fmt.format(duration) == "1 second")
  }

  test("second 3") {
    val duration = Duration.ofNanos(2_500_100_000L)
    assert(fmt.format(duration) == "2 seconds 500 milli")
  }

  test("minute") {
    val duration = Duration.ofMillis(65_900)
    assert(fmt.format(duration) == "1 minute 5 seconds")
  }
  test("minute 2") {
    val duration = Duration.ofSeconds(120)
    assert(fmt.format(duration) == "2 minutes")
  }
  test("minute 3") {
    val duration = Duration.ofSeconds(60)
    assert(fmt.format(duration) == "1 minute")
  }

  test("hour") {
    val duration = Duration.ofMinutes(67)
    assert(fmt.format(duration) == "1 hour 7 minutes")
  }

  test("hour 2") {
    val duration = Duration.ofSeconds(3659)
    assert(fmt.format(duration) == "1 hour")
  }
}
