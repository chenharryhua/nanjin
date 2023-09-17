package mtest.common

import com.github.chenharryhua.nanjin.common.chrono.{policies, Tick}
import org.scalatest.funsuite.AnyFunSuite

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, ZoneId, ZonedDateTime}
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

class PolicyTest extends AnyFunSuite {
  val interval: Duration = Duration.of(1, ChronoUnit.SECONDS)
  val t0: Instant        = ZonedDateTime.of(2023, 9, 16, 16, 30, 0, 0, ZoneId.systemDefault()).toInstant
  val zero: Tick = Tick(
    UUID.randomUUID(),
    t0,
    index = 0,
    counter = 0,
    previous = t0,
    snooze = Duration.ZERO,
    acquire = t0,
    guessNext = None)
  val t1: Instant = t0.plus(interval)
  val t2: Instant = t1.plus(interval)
  val t3: Instant = t2.plus(interval)
  val t4: Instant = t3.plus(interval)
  val t5: Instant = t4.plus(interval)
  val t6: Instant = t5.plus(interval)

  test("constant") {
    val delay  = 1.second.toJava
    val policy = policies.constant(delay).limited(3)

    val a1 = policy.decide(zero, t1).get
    val a2 = policy.decide(a1, t2).get
    val a3 = policy.decide(a2, t3).get
    val a4 = policy.decide(a3, t4)
    assert(a1.sequenceId == zero.sequenceId)
    assert(a1.launchTime == zero.launchTime)
    assert(a1.index == 1)
    assert(a1.counter == 1)
    assert(a1.previous === t0)
    assert(a1.acquire === t1)
    assert(a1.snooze === delay)

    assert(a2.sequenceId == zero.sequenceId)
    assert(a2.launchTime == zero.launchTime)
    assert(a2.index == 2)
    assert(a2.counter == 2)
    assert(a2.previous === a1.wakeup)
    assert(a2.acquire === t2)
    assert(a2.snooze === delay)

    assert(a3.sequenceId == zero.sequenceId)
    assert(a3.launchTime == zero.launchTime)
    assert(a3.index == 3)
    assert(a3.counter == 3)
    assert(a3.previous === a2.wakeup)
    assert(a3.acquire === t3)
    assert(a3.snooze === delay)

    assert(a4.isEmpty)
  }

  test("follow by") {
    val delay = 1.second.toJava
    val policy =
      policies.constant(delay).limited(3).followBy(policies.constant(delay.multipliedBy(2)).limited(5))
    println(policy.show)
    val a1 = policy.decide(zero, t1).get
    val a2 = policy.decide(a1, t2).get
    val a3 = policy.decide(a2, t3).get
    val a4 = policy.decide(a3, t4).get
    val a5 = policy.decide(a4, t5).get
    val a6 = policy.decide(a5, t6)

    assert(a1.snooze == 1.second.toJava)
    assert(a2.snooze == 1.second.toJava)
    assert(a3.snooze == 1.second.toJava)
    assert(a4.snooze == 2.seconds.toJava)
    assert(a5.snooze == 2.seconds.toJava)
    assert(a6.isEmpty)
  }

  test("repeat") {
    val delay = 1.second.toJava
    val policy =
      policies.constant(delay).limited(3).followBy(policies.constant(delay.multipliedBy(2)).limited(5)).repeat
    println(policy.show)
    val a1 = policy.decide(zero, t1).get
    val a2 = policy.decide(a1, t2).get
    val a3 = policy.decide(a2, t3).get
    val a4 = policy.decide(a3, t4).get
    val a5 = policy.decide(a4, t5).get
    val a6 = policy.decide(a5, t6).get

    assert(a1.snooze == 1.second.toJava)
    assert(a2.snooze == 1.second.toJava)
    assert(a3.snooze == 1.second.toJava)
    assert(a4.snooze == 2.seconds.toJava)
    assert(a5.snooze == 2.seconds.toJava)
    assert(a6.snooze == 1.second.toJava)
    assert(a6.counter == 1)
  }

  test("fixed pace") {
    val delay  = 10.minutes.toJava
    val policy = policies.fixedPace(delay)

    val a1 = policy.decide(zero, zero.launchTime.plus(5.minutes.toJava)).get
    assert(a1.wakeup == zero.launchTime.plus(delay))
    val a2 = policy.decide(a1, zero.launchTime.plus(15.minutes.toJava)).get
    assert(a2.wakeup == zero.launchTime.plus(delay.multipliedBy(2)))
  }

  test("exponential") {
    val policy = policies.exponential(1.second)
    val a1     = policy.decide(zero, t0).get
    assert(a1.snooze == 1.second.toJava)
    val a2 = policy.decide(a1, t1).get
    assert(a2.snooze == 2.second.toJava)
    val a3 = policy.decide(a2, t2).get
    assert(a3.snooze == 4.seconds.toJava)
    val a4 = policy.decide(a3, t3).get
    assert(a4.snooze == 8.seconds.toJava)
    val a5 = policy.decide(a4, t4).get
    assert(a5.snooze == 16.seconds.toJava)
  }

  test("fibonacci") {
    val policy = policies.fibonacci(1.minute)
    val a1     = policy.decide(zero, t0).get
    assert(a1.snooze == 1.minute.toJava)
    val a2 = policy.decide(a1, t1).get
    assert(a2.snooze == 1.minute.toJava)
    val a3 = policy.decide(a2, t2).get
    assert(a3.snooze == 2.minute.toJava)
    val a4 = policy.decide(a3, t3).get
    assert(a4.snooze == 3.minute.toJava)
    val a5 = policy.decide(a4, t4).get
    assert(a5.snooze == 5.minute.toJava)
    val a6 = policy.decide(a5, t5).get
    assert(a6.snooze == 8.minute.toJava)
    val a7 = policy.decide(a6, t6).get
    assert(a7.snooze == 13.minute.toJava)
  }

  test("jitter") {
    val policy = policies.jitter(1.minute, 2.hour)
    val a1     = policy.decide(zero, t0).get
    assert(a1.snooze.toScala >= 1.minute)
    assert(a1.snooze.toScala < 2.hour)
  }
}
