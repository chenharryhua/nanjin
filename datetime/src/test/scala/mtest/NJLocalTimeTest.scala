package mtest

import com.github.chenharryhua.nanjin.datetime.NJLocalTime
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalTime
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

class NJLocalTimeTest extends AnyFunSuite {
  test("local time distance") {
    val base       = NJLocalTime(LocalTime.of(18, 0))
    val localTime1 = LocalTime.of(19, 0)
    val localTime2 = LocalTime.of(17, 0)
    val localTime3 = LocalTime.of(18, 0)
    val localTime4 = LocalTime.of(17, 59, 59)
    assert(base.distance(localTime1) == FiniteDuration(1, TimeUnit.HOURS))
    assert(base.distance(localTime2) == FiniteDuration(23, TimeUnit.HOURS))
    assert(base.distance(localTime3) == FiniteDuration(0, TimeUnit.HOURS))
    assert(base.distance(localTime4) == FiniteDuration(24, TimeUnit.HOURS).minus(1.second))
  }
}
