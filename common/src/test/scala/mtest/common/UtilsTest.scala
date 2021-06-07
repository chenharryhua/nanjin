package mtest.common

import com.github.chenharryhua.nanjin.common.utils
import org.scalatest.funsuite.AnyFunSuite

import java.time.{Instant, LocalTime}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

class UtilsTest extends AnyFunSuite {
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
    val lowest = Duration(1000, TimeUnit.MILLISECONDS)
    assert(utils.mkDurationString(lowest) == "1 second")
    val arbi = Duration(123456789, TimeUnit.MILLISECONDS)
    assert(utils.mkDurationString(arbi) == "1 day 10 hours 17 minutes 36 seconds")
  }
  test("duration always positive") {
    val now   = Instant.now()
    val after = now.plusSeconds(100)

    assert(utils.mkDurationString(now, after) == utils.mkDurationString(after, now))
  }
  test("local time diff") {
    val base       = LocalTime.of(18, 0)
    val localTime1 = LocalTime.of(19, 0)
    val localTime2 = LocalTime.of(17, 0)
    val localTime3 = LocalTime.of(18, 0)
    val localTime4 = LocalTime.of(17, 59, 59)
    assert(utils.localTimeDiff(base, localTime1) == FiniteDuration(1, TimeUnit.HOURS))
    assert(utils.localTimeDiff(base, localTime2) == FiniteDuration(23, TimeUnit.HOURS))
    assert(utils.localTimeDiff(base, localTime3) == FiniteDuration(0, TimeUnit.HOURS))
    assert(utils.localTimeDiff(base, localTime4) == FiniteDuration(24, TimeUnit.HOURS).minus(1.second))
  }
}
