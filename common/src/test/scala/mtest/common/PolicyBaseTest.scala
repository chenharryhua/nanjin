package mtest.common

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.common.chrono.zones.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.*
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

class PolicyBaseTest extends AnyFunSuite {
  val interval: Duration   = Duration.of(1, ChronoUnit.SECONDS)
  val localDate: LocalDate = LocalDate.of(2023, 9, 16)
  val t0: Instant          = ZonedDateTime.of(localDate, localTimes.amEight, utcTime).toInstant
  val t1: Instant          = t0.plus(interval)
  val t2: Instant          = t1.plus(interval)
  val t3: Instant          = t2.plus(interval)
  val t4: Instant          = t3.plus(interval)
  val t5: Instant          = t4.plus(interval)
  val t6: Instant          = t5.plus(interval)

  test("fixed delay") {
    val delay  = 1.second.toJava
    val policy = policies.fixedDelay(1.second)
    println(policy.show)
    val ts   = TickStatus[IO](policy, beijingTime).unsafeRunSync()
    val zero = ts.tick
    val a1   = ts.next(t1).get
    val a2   = a1.next(t2).get
    val a3   = a2.next(t3).get
    val a4   = a3.next(t4).get

    assert(a1.tick.sequenceId == zero.sequenceId)
    assert(a1.tick.launchTime == zero.launchTime)
    assert(a1.tick.index == 1)
    assert(a1.tick.previous === zero.wakeup)
    assert(a1.tick.acquire === t1)
    assert(a1.tick.snooze === delay)

    assert(a2.tick.sequenceId == zero.sequenceId)
    assert(a2.tick.launchTime == zero.launchTime)
    assert(a2.tick.index == 2)
    assert(a2.tick.previous === a1.tick.wakeup)
    assert(a2.tick.acquire === t2)
    assert(a2.tick.snooze === delay)

    assert(a3.tick.sequenceId == zero.sequenceId)
    assert(a3.tick.launchTime == zero.launchTime)
    assert(a3.tick.index == 3)
    assert(a3.tick.previous === a2.tick.wakeup)
    assert(a3.tick.acquire === t3)
    assert(a3.tick.snooze === delay)

    assert(a4.tick.sequenceId == zero.sequenceId)
    assert(a4.tick.launchTime == zero.launchTime)
    assert(a4.tick.index == 4)
    assert(a4.tick.previous === a3.tick.wakeup)
    assert(a4.tick.acquire === t4)
    assert(a4.tick.snooze === delay)

  }

  test("fixed rate") {
    val delay  = 10.minutes.toJava
    val policy = policies.fixedRate(10.minutes)
    println(policy.show)
    val ts   = TickStatus[IO](policy, utcTime).unsafeRunSync()
    val zero = ts.tick
    val a1   = ts.next(zero.launchTime.plus(5.minutes.toJava)).get
    val a2   = a1.next(zero.launchTime.plus(15.minutes.toJava)).get
    val a3   = a2.next(zero.launchTime.plus(20.minutes.toJava)).get

    assert(a1.tick.index == 1)
    assert(a2.tick.index == 2)
    assert(a3.tick.index == 3)

    assert(a1.tick.wakeup == zero.launchTime.plus(delay))
    assert(a2.tick.wakeup == zero.launchTime.plus(delay.multipliedBy(2)))
    assert(a3.tick.wakeup == zero.launchTime.plus(delay.multipliedBy(3)))
    assert(a3.tick.snooze == delay)
  }



  test("jitter") {
    val policy = policies.jitter(1.minute, 2.hour)
    println(policy.show)
    val ts = TickStatus[IO](policy, newyorkTime).unsafeRunSync()
    val a1 = ts.next(t0).get.tick
    assert(a1.snooze.toScala >= 1.minute)
    assert(a1.snooze.toScala < 2.hour)
  }

  test("fixed delays") {
    val policy = policies.fixedDelay(1.second, 2.seconds, 3.seconds)
    println(policy.show)
    val ts = TickStatus[IO](policy, mumbaiTime).unsafeRunSync()
    val a1 = ts.next(t0).get
    val a2 = a1.next(t0).get
    val a3 = a2.next(t0).get
    val a4 = a3.next(t0).get
    val a5 = a4.next(t0).get
    val a6 = a5.next(t0).get
    val a7 = a6.next(t0).get

    assert(a1.tick.index == 1)
    assert(a2.tick.index == 2)
    assert(a3.tick.index == 3)
    assert(a4.tick.index == 4)
    assert(a5.tick.index == 5)
    assert(a6.tick.index == 6)
    assert(a7.tick.index == 7)

    assert(a1.tick.snooze == 1.second.toJava)
    assert(a2.tick.snooze == 2.second.toJava)
    assert(a3.tick.snooze == 3.second.toJava)
    assert(a4.tick.snooze == 1.second.toJava)
    assert(a5.tick.snooze == 2.second.toJava)
    assert(a6.tick.snooze == 3.second.toJava)
    assert(a7.tick.snooze == 1.second.toJava)
  }

  test("cron") {
    val policy = policies.crontab(crontabs.hourly)
    println(policy.show)
    val ts = TickStatus[IO](policy, beijingTime).unsafeRunSync()
    println(ts)
  }

}
