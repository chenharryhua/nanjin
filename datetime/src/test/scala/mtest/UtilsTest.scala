package mtest

import com.github.chenharryhua.nanjin.datetime.utils
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

class UtilsTest extends AnyFunSuite {
  test("duration always positive") {
    val now   = Instant.now()
    val after = now.plusSeconds(100)

    assert(utils.mkDurationString(now, after) == utils.mkDurationString(after, now))
  }
  test("nano") {
    val highest = Duration(999999, TimeUnit.NANOSECONDS)
    assert(utils.mkDurationString(highest) == "999999 nanoseconds")
  }
  test("milli") {
    val lowest = Duration(1000000, TimeUnit.NANOSECONDS)
    assert(utils.mkDurationString(lowest) == "1 milliseconds")
    val highest = Duration(999, TimeUnit.MILLISECONDS)
    assert(utils.mkDurationString(highest) == "999 milliseconds")
  }

  test("second") {
    val d1 = Duration(1000, TimeUnit.MILLISECONDS)
    assert(utils.mkDurationString(d1) == "1 second 0 milliseconds")
    val d2 = Duration(1234, TimeUnit.MILLISECONDS)
    assert(utils.mkDurationString(d2) == "1 second 234 milliseconds")
    val d3 = Duration(3234, TimeUnit.MILLISECONDS)
    assert(utils.mkDurationString(d3) == "3 seconds 234 milliseconds")
  }
  test("minute") {
    val d1 = Duration(60, TimeUnit.SECONDS)
    assert(utils.mkDurationString(d1) == "1 minute")
    val d2 = Duration(61123, TimeUnit.MILLISECONDS)
    assert(utils.mkDurationString(d2) == "1 minute 1 second")
    val d3 = Duration(123123, TimeUnit.MILLISECONDS)
    assert(utils.mkDurationString(d3) == "2 minutes 3 seconds")
  }
  test("hour") {
    val d1 = Duration(3600, TimeUnit.SECONDS)
    assert(utils.mkDurationString(d1) == "1 hour")
    val d2 = Duration(3789, TimeUnit.SECONDS)
    assert(utils.mkDurationString(d2) == "1 hour 3 minutes")
    val d3 = Duration(3611, TimeUnit.SECONDS)
    assert(utils.mkDurationString(d3) == "1 hour")
  }
  test("day") {
    val d1 = Duration(3600 * 24, TimeUnit.SECONDS)
    assert(utils.mkDurationString(d1) == "1 day")
    val d2 = Duration(3600 * 24 + 1234, TimeUnit.SECONDS)
    assert(utils.mkDurationString(d2) == "1 day")
    val d3 = Duration(3600 * 24 + 7200, TimeUnit.SECONDS)
    assert(utils.mkDurationString(d3) == "1 day 2 hours")
  }

  test("days") {
    val d1 = Duration(3600 * 24, TimeUnit.SECONDS)
    assert(utils.mkDurationString(d1) == "1 day")
    val d2 = Duration(3600 * 24 + 1234, TimeUnit.SECONDS)
    assert(utils.mkDurationString(d2) == "1 day")
    val d3 = Duration(3600 * 24 + 7200, TimeUnit.SECONDS)
    assert(utils.mkDurationString(d3) == "1 day 2 hours")
    val d4 = Duration(3600 * 24 * 5 + 3600, TimeUnit.SECONDS)
    assert(utils.mkDurationString(d4) == "5 days 1 hour")
  }
  test("month") {
    val d1 = Duration(30, TimeUnit.DAYS)
    assert(utils.mkDurationString(d1) == "30 days")
    val d2 = Duration(260, TimeUnit.DAYS).plus(Duration(30, TimeUnit.HOURS))
    assert(utils.mkDurationString(d2) == "261 days 6 hours")
    val d3 = Duration(450, TimeUnit.DAYS).plus(Duration(30, TimeUnit.HOURS))
    assert(utils.mkDurationString(d3) == "451 days 6 hours")
  }
}
