package mtest.common

import com.github.chenharryhua.nanjin.common.chrono.crontabs
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import cron4s.syntax.all.*
import munit.FunSuite

import java.time.{DayOfWeek, LocalDateTime}

class CrontabsTest extends FunSuite {
  test("1.validate crontabs") {
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
  test("2.yearly") {
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

  // Fires of `expr` during the 7 days starting at a fixed Monday 00:00, as (day, hour) pairs.
  private def weekFires(expr: CronExpr): List[(DayOfWeek, Int)] = {
    val monday = LocalDateTime.of(2026, 11, 9, 0, 0)
    assertEquals(monday.getDayOfWeek, DayOfWeek.MONDAY)
    val end = monday.plusDays(7)
    Iterator
      .iterate(expr.next(monday))(_.flatMap(expr.next(_)))
      .takeWhile(_.exists(_.isBefore(end)))
      .flatten
      .map(t => (t.getDayOfWeek, t.getHour))
      .toList
  }

  private val weekdays: List[DayOfWeek] =
    List(DayOfWeek.MONDAY, DayOfWeek.TUESDAY, DayOfWeek.WEDNESDAY, DayOfWeek.THURSDAY, DayOfWeek.FRIDAY)

  test("3.z9w5 fires 9:00 and 17:00 Monday to Friday only") {
    assertEquals(weekFires(crontabs.z9w5), weekdays.flatMap(d => List((d, 9), (d, 17))))
  }

  test("4.c996 fires 9:00 and 21:00 Monday to Saturday only") {
    assertEquals(
      weekFires(crontabs.c996),
      (weekdays :+ DayOfWeek.SATURDAY).flatMap(d => List((d, 9), (d, 21))))
  }

  test("5.c997 fires 9:00 and 21:00 every day") {
    assertEquals(weekFires(crontabs.c997).map(_._1).distinct.size, 7)
    assertEquals(weekFires(crontabs.c997).size, 14)
  }
}
