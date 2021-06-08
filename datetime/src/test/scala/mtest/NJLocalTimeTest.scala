package mtest

import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJLocalTime, NJLocalTimeRange}
import org.scalatest.funsuite.AnyFunSuite

import java.time.{Instant, LocalDateTime, LocalTime, ZonedDateTime}
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
  test("local time range - do not cross midnight") {
    val ltr = NJLocalTimeRange(LocalTime.of(8, 0), FiniteDuration(8, TimeUnit.HOURS), sydneyTime)
    val d1  = LocalDateTime.of(2012, 10, 22, 8, 0, 0).atZone(sydneyTime).toInstant
    val d2  = LocalDateTime.of(2015, 7, 25, 16, 0, 0).atZone(sydneyTime).toInstant
    assert(ltr.isInBetween(d1))
    assert(!ltr.isInBetween(d2))

    val d3 = LocalDateTime.of(2013, 9, 23, 15, 0, 0).atZone(sydneyTime).toInstant
    val d4 = LocalDateTime.of(2014, 8, 24, 15, 59, 59).atZone(sydneyTime).toInstant
    val d5 = LocalDateTime.of(2016, 6, 26, 17, 0, 0).atZone(sydneyTime).toInstant
    assert(ltr.isInBetween(d3))
    assert(ltr.isInBetween(d4))
    assert(!ltr.isInBetween(d5))
  }
  test("local time range - cross midnight") {
    val ltr = NJLocalTimeRange(LocalTime.of(22, 0), FiniteDuration(8, TimeUnit.HOURS), sydneyTime)
    val d1  = LocalDateTime.of(2012, 1, 1, 22, 0, 0).atZone(sydneyTime).toInstant
    val d2  = LocalDateTime.of(2013, 2, 2, 6, 0, 0).atZone(sydneyTime).toInstant
    assert(ltr.isInBetween(d1))
    assert(!ltr.isInBetween(d2))

    val d3 = LocalDateTime.of(2014, 3, 3, 23, 59, 59).atZone(sydneyTime).toInstant
    val d4 = LocalDateTime.of(2015, 4, 4, 0, 0, 0).atZone(sydneyTime).toInstant
    val d5 = LocalDateTime.of(2015, 4, 4, 0, 0, 1).atZone(sydneyTime).toInstant
    val d6 = LocalDateTime.of(2015, 4, 4, 12, 0, 0).atZone(sydneyTime).toInstant
    assert(ltr.isInBetween(d3))
    assert(ltr.isInBetween(d4))
    assert(ltr.isInBetween(d5))
    assert(!ltr.isInBetween(d6))
  }
  test("local time range - start time match") {
    val ltr = NJLocalTimeRange(LocalTime.of(22, 0), FiniteDuration(2, TimeUnit.HOURS), sydneyTime)
    val d1  = LocalDateTime.of(2012, 1, 1, 22, 0, 0).atZone(sydneyTime).toInstant
    val d2  = LocalDateTime.of(2013, 2, 2, 0, 0, 0).atZone(sydneyTime).toInstant
    assert(ltr.isInBetween(d1))
    assert(!ltr.isInBetween(d2))

    val d3 = LocalDateTime.of(2014, 3, 3, 23, 59, 59).atZone(sydneyTime).toInstant
    val d4 = LocalDateTime.of(2014, 3, 3, 1, 0, 0).atZone(sydneyTime).toInstant
    assert(ltr.isInBetween(d3))
    assert(!ltr.isInBetween(d4))
  }

  test("duration <= 0") {
    val ltr = NJLocalTimeRange(LocalTime.of(22, 0), FiniteDuration(-2, TimeUnit.HOURS), sydneyTime)
    val d1  = LocalDateTime.of(2012, 1, 1, 21, 0, 0).atZone(sydneyTime).toInstant
    assert(!ltr.isInBetween(d1))
  }

  test("duration >= 24 hours") {
    val ltr = NJLocalTimeRange(LocalTime.of(22, 0), FiniteDuration(24, TimeUnit.HOURS), sydneyTime)
    val d1  = LocalDateTime.of(2012, 1, 1, 22, 0, 0).atZone(sydneyTime).toInstant
    assert(ltr.isInBetween(d1))
  }
}
