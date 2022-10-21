package mtest

import com.github.chenharryhua.nanjin.datetime.crontabs
import cron4s.lib.javatime.javaTemporalInstance
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime

class CrontabsTest extends AnyFunSuite {
  test("validate crontabs") {
    val now = LocalDateTime.now()
    assert(crontabs.everySecond.next(now).nonEmpty)
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

    assert(crontabs.slack.next(now).nonEmpty)
  }
}
