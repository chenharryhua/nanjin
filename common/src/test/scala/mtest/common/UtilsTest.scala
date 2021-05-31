package mtest.common

import com.github.chenharryhua.nanjin.common.utils
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

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
}
