package mtest.common

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.chrono.zones.{saltaTime, sydneyTime}
import com.github.chenharryhua.nanjin.common.chrono.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.*
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

class PolicyTest extends AnyFunSuite {
  val interval: Duration   = Duration.of(1, ChronoUnit.SECONDS)
  val localDate: LocalDate = LocalDate.of(2023, 9, 16)
  val zoneId: ZoneId       = saltaTime
  val t0: Instant          = ZonedDateTime.of(localDate, localTimes.amEight, zoneId).toInstant
  val t1: Instant          = t0.plus(interval)
  val t2: Instant          = t1.plus(interval)
  val t3: Instant          = t2.plus(interval)
  val t4: Instant          = t3.plus(interval)
  val t5: Instant          = t4.plus(interval)
  val t6: Instant          = t5.plus(interval)

  test("constant") {
    val delay  = 1.second.toJava
    val policy = policies.accordance(policies.constant(delay).limited(3))
    println(policy.show)
    val ts   = TickStatus[IO](policy, zoneId).unsafeRunSync()
    val zero = ts.tick
    val a1   = ts.next(t1).get
    val a2   = a1.next(t2).get
    val a3   = a2.next(t3).get
    val a4   = a3.next(t4)

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

    assert(a4.isEmpty)
  }

  test("follow by") {
    val delay = 1.second.toJava
    val policy =
      policies.constant(delay).limited(3).followedBy(policies.constant(delay.multipliedBy(2)).limited(2))
    println(policy.show)
    val ts = TickStatus[IO](policy, zoneId).unsafeRunSync()
    val a1 = ts.next(t1).get
    val a2 = a1.next(t2).get
    val a3 = a2.next(t3).get
    val a4 = a3.next(t4).get
    val a5 = a4.next(t5).get
    val a6 = a5.next(t6)

    assert(a1.tick.index == 1)
    assert(a2.tick.index == 2)
    assert(a3.tick.index == 3)
    assert(a4.tick.index == 4)
    assert(a5.tick.index == 5)

    assert(a1.tick.snooze == 1.second.toJava)
    assert(a2.tick.snooze == 1.second.toJava)
    assert(a3.tick.snooze == 1.second.toJava)
    assert(a4.tick.snooze == 2.seconds.toJava)
    assert(a5.tick.snooze == 2.seconds.toJava)
    assert(a6.isEmpty)
  }

  test("repeat") {
    val delay = 1.second.toJava
    val policy =
      policies
        .accordance(policies.constant(delay).limited(3))
        .followedBy(policies.constant(delay.multipliedBy(2)).limited(2))
        .repeat

    println(policy.show)
    val ts = TickStatus[IO](policy, zoneId).unsafeRunSync()
    val a1 = ts.next(t1).get
    val a2 = a1.next(t2).get
    val a3 = a2.next(t3).get
    val a4 = a3.next(t4).get
    val a5 = a4.next(t5).get
    val a6 = a5.next(t6).get

    assert(a1.tick.index == 1)
    assert(a2.tick.index == 2)
    assert(a3.tick.index == 3)
    assert(a4.tick.index == 4)
    assert(a5.tick.index == 5)
    assert(a6.tick.index == 6)

    assert(a1.tick.snooze == 1.second.toJava)
    assert(a2.tick.snooze == 1.second.toJava)
    assert(a3.tick.snooze == 1.second.toJava)
    assert(a4.tick.snooze == 2.seconds.toJava)
    assert(a5.tick.snooze == 2.seconds.toJava)
    assert(a6.tick.snooze == 1.second.toJava)
  }

  test("fixed pace") {
    val delay  = 10.minutes.toJava
    val policy = policies.fixedPace(delay)
    println(policy.show)
    val ts   = TickStatus[IO](policy, zoneId).unsafeRunSync()
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

  test("exponential") {
    val policy = policies.exponential(1.second)
    println(policy.show)
    val ts = TickStatus[IO](policy, zoneId).unsafeRunSync()
    val a1 = ts.next(t0).get
    val a2 = a1.next(t0).get
    val a3 = a2.next(t0).get
    val a4 = a3.next(t0).get
    val a5 = a4.next(t0).get

    assert(a1.tick.index == 1)
    assert(a2.tick.index == 2)
    assert(a3.tick.index == 3)
    assert(a4.tick.index == 4)
    assert(a5.tick.index == 5)

    assert(a1.tick.snooze == 1.second.toJava)
    assert(a2.tick.snooze == 2.second.toJava)
    assert(a3.tick.snooze == 4.seconds.toJava)
    assert(a4.tick.snooze == 8.seconds.toJava)
    assert(a5.tick.snooze == 16.seconds.toJava)
  }

  test("fibonacci") {
    val policy = policies.fibonacci(1.minute)
    println(policy.show)

    val ts = TickStatus[IO](policy, zoneId).unsafeRunSync()
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

    assert(a1.tick.snooze == 1.minute.toJava)
    assert(a2.tick.snooze == 1.minute.toJava)
    assert(a3.tick.snooze == 2.minute.toJava)
    assert(a4.tick.snooze == 3.minute.toJava)
    assert(a5.tick.snooze == 5.minute.toJava)
    assert(a6.tick.snooze == 8.minute.toJava)
    assert(a7.tick.snooze == 13.minute.toJava)
  }

  test("capped") {
    val policy = policies.fibonacci(1.minute).capped(6.minutes)
    println(policy.show)
    val ts = TickStatus[IO](policy, zoneId).unsafeRunSync()
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

    assert(a1.tick.snooze == 1.minute.toJava)
    assert(a2.tick.snooze == 1.minute.toJava)
    assert(a3.tick.snooze == 2.minute.toJava)
    assert(a4.tick.snooze == 3.minute.toJava)
    assert(a5.tick.snooze == 5.minute.toJava)
    assert(a6.tick.snooze == 6.minute.toJava)
    assert(a7.tick.snooze == 6.minute.toJava)
  }

  test("threshold") {
    val policy = policies.fibonacci(1.minute).threshold(6.minute)
    println(policy.show)
    val ts = TickStatus[IO](policy, zoneId).unsafeRunSync()
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

    assert(a1.tick.snooze == 1.minute.toJava)
    assert(a2.tick.snooze == 1.minute.toJava)
    assert(a3.tick.snooze == 2.minute.toJava)
    assert(a4.tick.snooze == 3.minute.toJava)
    assert(a5.tick.snooze == 5.minute.toJava)
    assert(a6.tick.snooze == 1.minute.toJava)
    assert(a7.tick.snooze == 1.minute.toJava)
  }

  test("jitter") {
    val policy = policies.jitter(1.minute, 2.hour)
    println(policy.show)
    val ts = TickStatus[IO](policy, zoneId).unsafeRunSync()
    val a1 = ts.next(t0).get.tick
    assert(a1.snooze.toScala >= 1.minute)
    assert(a1.snooze.toScala < 2.hour)
  }

  test("delays") {
    val policy = policies.delays(1.second, 2.seconds, 3.seconds).repeat
    println(policy.show)
    val ts = TickStatus[IO](policy, zoneId).unsafeRunSync()
    val a1 = ts.next(t0).get
    val a2 = a1.next(t0).get
    val a3 = a2.next(t0).get
    val a4 = a3.next(t0).get

    assert(a1.tick.index == 1)
    assert(a2.tick.index == 2)
    assert(a3.tick.index == 3)
    assert(a4.tick.index == 4)

    assert(a1.tick.snooze == 1.second.toJava)
    assert(a2.tick.snooze == 2.second.toJava)
    assert(a3.tick.snooze == 3.second.toJava)
    assert(a4.tick.snooze == 1.second.toJava)
  }

  ignore("endup") {
    val time = LocalTime.of(11, 25, 0)
    val policy = policies
      .crontab(crontabs.every10Seconds)
      .endUp(time)
      .followedBy(policies.constant(1.second).endUp(time.plus(5.seconds.toJava)))
      .followedBy(policies.exponential(1.second).endOfDay)
      .repeat
    println(policy)
    tickStream[IO](policy, sydneyTime).debug().compile.drain.unsafeRunSync()
  }
}
