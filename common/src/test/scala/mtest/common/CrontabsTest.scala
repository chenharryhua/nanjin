package mtest.common

import com.github.chenharryhua.nanjin.common.chrono.crontabs
import cron4s.lib.javatime.javaTemporalInstance
import cron4s.syntax.all.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime

class CrontabsTest extends AnyFunSuite {
  test("validate crontabs") {
    val now = LocalDateTime.now()
    assert(crontabs.everySecond.next(now).nonEmpty)
    assert(crontabs.secondly.next(now).nonEmpty)
    assert(crontabs.every2Seconds.next(now).nonEmpty)
    assert(crontabs.every3Seconds.next(now).nonEmpty)
    assert(crontabs.every4Seconds.next(now).nonEmpty)
    assert(crontabs.every5Seconds.next(now).nonEmpty)
    assert(crontabs.every6Seconds.next(now).nonEmpty)
    assert(crontabs.every10Seconds.next(now).nonEmpty)
    assert(crontabs.every12Seconds.next(now).nonEmpty)
    assert(crontabs.every15Seconds.next(now).nonEmpty)
    assert(crontabs.every20Seconds.next(now).nonEmpty)
    assert(crontabs.every30Seconds.next(now).nonEmpty)

    assert(crontabs.everyMinute.next(now).nonEmpty)
    assert(crontabs.minutely.next(now).nonEmpty)
    assert(crontabs.every2Minutes.next(now).nonEmpty)
    assert(crontabs.every3Minutes.next(now).nonEmpty)
    assert(crontabs.every4Minutes.next(now).nonEmpty)
    assert(crontabs.every5Minutes.next(now).nonEmpty)
    assert(crontabs.every6Minutes.next(now).nonEmpty)
    assert(crontabs.every10Minutes.next(now).nonEmpty)
    assert(crontabs.every12Minutes.next(now).nonEmpty)
    assert(crontabs.every15Minutes.next(now).nonEmpty)
    assert(crontabs.every20Minutes.next(now).nonEmpty)
    assert(crontabs.every30Minutes.next(now).nonEmpty)

    assert(crontabs.everyHour.next(now).nonEmpty)
    assert(crontabs.hourly.next(now).nonEmpty)
    assert(crontabs.every2Hours.next(now).nonEmpty)
    assert(crontabs.every3Hours.next(now).nonEmpty)
    assert(crontabs.every4Hours.next(now).nonEmpty)
    assert(crontabs.every6Hours.next(now).nonEmpty)
    assert(crontabs.every8Hours.next(now).nonEmpty)
    assert(crontabs.every12Hours.next(now).nonEmpty)

    assert(crontabs.z9w5.next(now).nonEmpty)
    assert(crontabs.c996.next(now).nonEmpty)
    assert(crontabs.c997.next(now).nonEmpty)

    assert(crontabs.businessHour.next(now).nonEmpty)
  }
  test("yearly") {
    assert(crontabs.yearly.january.next(LocalDateTime.now).get.toString.drop(5) == "01-01T00:00")
    assert(crontabs.yearly.february.next(LocalDateTime.now).get.toString.drop(5) == "02-01T00:00")
    assert(crontabs.yearly.march.next(LocalDateTime.now).get.toString.drop(5) == "03-01T00:00")
    assert(crontabs.yearly.april.next(LocalDateTime.now).get.toString.drop(5) == "04-01T00:00")
    assert(crontabs.yearly.may.next(LocalDateTime.now).get.toString.drop(5) == "05-01T00:00")
    assert(crontabs.yearly.june.next(LocalDateTime.now).get.toString.drop(5) == "06-01T00:00")
    assert(crontabs.yearly.july.next(LocalDateTime.now).get.toString.drop(5) == "07-01T00:00")
    assert(crontabs.yearly.august.next(LocalDateTime.now).get.toString.drop(5) == "08-01T00:00")
    assert(crontabs.yearly.september.next(LocalDateTime.now).get.toString.drop(5) == "09-01T00:00")
    assert(crontabs.yearly.october.next(LocalDateTime.now).get.toString.drop(5) == "10-01T00:00")
    assert(crontabs.yearly.november.next(LocalDateTime.now).get.toString.drop(5) == "11-01T00:00")
    assert(crontabs.yearly.december.next(LocalDateTime.now).get.toString.drop(5) == "12-01T00:00")
  }
}
