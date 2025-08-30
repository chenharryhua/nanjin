package mtest.common

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.common.sequence.*
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import java.time.{DayOfWeek, Instant, Month}
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps
class PolicyBaseTest extends AnyFunSuite {

  test("fibonacci") {
    assert(fibonacci.take(10).toList == List(1, 1, 2, 3, 5, 8, 13, 21, 34, 55))
    assert(exponential.take(10).toList == List(1, 2, 4, 8, 16, 32, 64, 128, 256, 512))
    assert(primes.take(10).toList == List(2, 3, 5, 7, 11, 13, 17, 19, 23, 29))
  }

  test("fixed delay") {
    val policy = Policy.fixedDelay(1.second, 0.second)
    println(policy.show)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ts: TickStatus = zeroTickStatus.renewPolicy(policy)

    val List(a1, a2, a3, a4, a5) = tickLazyList.fromTickStatus(ts).take(5).toList

    assert(a1.sequenceId == ts.tick.sequenceId)
    assert(a1.launchTime == ts.tick.launchTime)
    assert(a1.index == 1)
    assert(a1.commence === ts.tick.conclude)
    assert(a1.snooze == 1.second.toJava)

    assert(a2.sequenceId == ts.tick.sequenceId)
    assert(a2.launchTime == ts.tick.launchTime)
    assert(a2.index == 2)
    assert(a2.commence === a1.conclude)
    assert(a2.snooze == 0.second.toJava)

    assert(a3.sequenceId == ts.tick.sequenceId)
    assert(a3.launchTime == ts.tick.launchTime)
    assert(a3.index == 3)
    assert(a3.commence === a2.conclude)
    assert(a3.snooze == 1.second.toJava)

    assert(a4.sequenceId == ts.tick.sequenceId)
    assert(a4.launchTime == ts.tick.launchTime)
    assert(a4.index == 4)
    assert(a4.commence === a3.conclude)
    assert(a4.snooze == 0.second.toJava)

    assert(a5.sequenceId == ts.tick.sequenceId)
    assert(a5.launchTime == ts.tick.launchTime)
    assert(a5.index == 5)
    assert(a5.commence === a4.conclude)
    assert(a5.snooze == 1.second.toJava)
    assert(List(a1, a2, a3, a4, a5).forall(t => t.acquires.plus(t.snooze) == t.conclude))
  }

  test("fixed rate") {
    val policy = Policy.fixedRate(1.second)
    println(policy.show)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ts: TickStatus = zeroTickStatus.renewPolicy(policy)

    val List(a1, a2, a3, a4, a5) = tickLazyList.fromTickStatus(ts).take(5).toList

    assert(a1.sequenceId == ts.tick.sequenceId)
    assert(a1.launchTime == ts.tick.launchTime)
    assert(a1.index == 1)
    assert(a1.commence === ts.tick.conclude)
    assert(a1.conclude == a1.commence.plus(1.seconds.toJava))

    assert(a2.sequenceId == ts.tick.sequenceId)
    assert(a2.launchTime == ts.tick.launchTime)
    assert(a2.index == 2)
    assert(a2.commence === a1.conclude)
    assert(a2.conclude == a2.commence.plus(1.seconds.toJava))

    assert(a3.sequenceId == ts.tick.sequenceId)
    assert(a3.launchTime == ts.tick.launchTime)
    assert(a3.index == 3)
    assert(a3.commence === a2.conclude)
    assert(a3.conclude == a3.commence.plus(1.seconds.toJava))

    assert(a4.sequenceId == ts.tick.sequenceId)
    assert(a4.launchTime == ts.tick.launchTime)
    assert(a4.index == 4)
    assert(a4.commence === a3.conclude)
    assert(a4.conclude == a4.commence.plus(1.seconds.toJava))

    assert(a5.sequenceId == ts.tick.sequenceId)
    assert(a5.launchTime == ts.tick.launchTime)
    assert(a5.index == 5)
    assert(a5.commence === a4.conclude)
    assert(a5.conclude == a5.commence.plus(1.seconds.toJava))
    assert(List(a1, a2, a3, a4, a5).forall(t => t.acquires.plus(t.snooze) == t.conclude))
  }

  test("fixed delays") {
    val policy = Policy.fixedDelay(1.second, 2.seconds, 3.seconds)
    println(policy.show)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ts: TickStatus = zeroTickStatus.renewPolicy(policy)

    val List(a1, a2, a3, a4, a5, a6, a7) = tickLazyList.fromTickStatus(ts).take(7).toList

    assert(a1.index == 1)
    assert(a2.index == 2)
    assert(a3.index == 3)
    assert(a4.index == 4)
    assert(a5.index == 5)
    assert(a6.index == 6)
    assert(a7.index == 7)

    assert(a1.snooze == 1.second.toJava)
    assert(a2.snooze == 2.second.toJava)
    assert(a3.snooze == 3.second.toJava)
    assert(a4.snooze == 1.second.toJava)
    assert(a5.snooze == 2.second.toJava)
    assert(a6.snooze == 3.second.toJava)
    assert(a7.snooze == 1.second.toJava)
    assert(List(a1, a2, a3, a4, a5, a6, a7).forall(t => t.acquires.plus(t.snooze) == t.conclude))
  }

  test("cron") {
    val policy = Policy.crontab(_.hourly)
    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ts = zeroTickStatus.renewPolicy(policy)

    val a1 = ts.next(ts.tick.launchTime.plus(1.hour.toJava)).get
    val a2 = a1.next(a1.tick.conclude.plus(30.minutes.toJava)).get
    val a3 = a2.next(a2.tick.conclude.plus(45.minutes.toJava)).get
    val a4 = a3.next(a2.tick.conclude.plus(60.minutes.toJava)).get

    assert(a1.tick.index == 1)
    assert(a2.tick.index == 2)
    assert(a3.tick.index == 3)
    assert(a4.tick.index == 4)

    assert(a2.tick.snooze == 30.minutes.toJava)
    assert(a3.tick.snooze == 15.minutes.toJava)
    assert(a4.tick.snooze == 1.hour.toJava)

    assert(a4.tick.isWithinClosedOpen(a4.tick.commence))
    assert(!a4.tick.isWithinClosedOpen(a4.tick.conclude))

    assert(a4.tick.isWithinOpenClosed(a4.tick.conclude))
    assert(!a4.tick.isWithinOpenClosed(a4.tick.commence))
  }

  test("giveUp") {
    val policy = Policy.giveUp
    println(policy.show)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ts = zeroTickStatus.renewPolicy(policy)
    assert(ts.next(Instant.now).isEmpty)
  }

  test("weekly") {
    val sunday = tickLazyList.from(Policy.crontab(_.weekly.sunday)).take(1).head.zoned(_.conclude)
    assert(DayOfWeek.from(sunday) == DayOfWeek.SUNDAY)

    val monday = tickLazyList.from(Policy.crontab(_.weekly.monday)).take(1).head.zoned(_.conclude)
    assert(DayOfWeek.from(monday) == DayOfWeek.MONDAY)

    val tuesday = tickLazyList.from(Policy.crontab(_.weekly.tuesday)).take(1).head.zoned(_.conclude)
    assert(DayOfWeek.from(tuesday) == DayOfWeek.TUESDAY)

    val wednesday = tickLazyList.from(Policy.crontab(_.weekly.wednesday)).take(1).head.zoned(_.conclude)
    assert(DayOfWeek.from(wednesday) == DayOfWeek.WEDNESDAY)

    val thursday = tickLazyList.from(Policy.crontab(_.weekly.thursday)).take(1).head.zoned(_.conclude)
    assert(DayOfWeek.from(thursday) == DayOfWeek.THURSDAY)

    val friday = tickLazyList.from(Policy.crontab(_.weekly.friday)).take(1).head.zoned(_.conclude)
    assert(DayOfWeek.from(friday) == DayOfWeek.FRIDAY)

    val saturday = tickLazyList.from(Policy.crontab(_.weekly.saturday)).take(1).head.zoned(_.conclude)
    assert(DayOfWeek.from(saturday) == DayOfWeek.SATURDAY)
  }

  test("yearly") {
    val january = tickLazyList.from(Policy.crontab(_.yearly.january)).take(1).head.zoned(_.conclude)
    assert(Month.from(january) == Month.JANUARY)

    val february = tickLazyList.from(Policy.crontab(_.yearly.february)).take(1).head.zoned(_.conclude)
    assert(Month.from(february) == Month.FEBRUARY)

    val march = tickLazyList.from(Policy.crontab(_.yearly.march)).take(1).head.zoned(_.conclude)
    assert(Month.from(march) == Month.MARCH)

    val april = tickLazyList.from(Policy.crontab(_.yearly.april)).take(1).head.zoned(_.conclude)
    assert(Month.from(april) == Month.APRIL)

    val may = tickLazyList.from(Policy.crontab(_.yearly.may)).take(1).head.zoned(_.conclude)
    assert(Month.from(may) == Month.MAY)

    val june = tickLazyList.from(Policy.crontab(_.yearly.june)).take(1).head.zoned(_.conclude)
    assert(Month.from(june) == Month.JUNE)

    val july = tickLazyList.from(Policy.crontab(_.yearly.july)).take(1).head.zoned(_.conclude)
    assert(Month.from(july) == Month.JULY)

    val august = tickLazyList.from(Policy.crontab(_.yearly.august)).take(1).head.zoned(_.conclude)
    assert(Month.from(august) == Month.AUGUST)

    val september = tickLazyList.from(Policy.crontab(_.yearly.september)).take(1).head.zoned(_.conclude)
    assert(Month.from(september) == Month.SEPTEMBER)

    val october = tickLazyList.from(Policy.crontab(_.yearly.october)).take(1).head.zoned(_.conclude)
    assert(Month.from(october) == Month.OCTOBER)

    val november = tickLazyList.from(Policy.crontab(_.yearly.november)).take(1).head.zoned(_.conclude)
    assert(Month.from(november) == Month.NOVEMBER)

    val december = tickLazyList.from(Policy.crontab(_.yearly.december)).take(1).head.zoned(_.conclude)
    assert(Month.from(december) == Month.DECEMBER)

  }
}
