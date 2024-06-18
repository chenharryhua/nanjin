package mtest.common

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.chrono.{
  crontabs,
  localTimes,
  policies,
  tickLazyList,
  Policy,
  TickStatus
}
import cron4s.CronExpr
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

class PolicyCombinatorTest extends AnyFunSuite {

  test("simple followed by") {
    val policy = policies.giveUp.followedBy(policies.fixedDelay(1.second))
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)
  }

  test("accordance") {
    val policy = policies.accordance(policies.fixedDelay(1.second))
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)
  }

  test("follow by") {
    val policy = policies
      .accordance(policies.fixedDelay(1.second).limited(3))
      .followedBy(policies.fixedDelay(2.seconds).limited(2))

    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ts = zeroTickStatus.renewPolicy(policy)
    val t0 = ts.tick.launchTime
    val a1 = ts.next(t0).get
    val a2 = a1.next(t0).get
    val a3 = a2.next(t0).get
    val a4 = a3.next(t0).get
    val a5 = a4.next(t0).get
    val a6 = a5.next(t0)

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
    val policy =
      policies
        .accordance(policies.fixedDelay(1.second).limited(1).repeat.limited(3))
        .followedBy(policies.fixedDelay(2.seconds).limited(2))
        .repeat

    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ts: TickStatus = zeroTickStatus.renewPolicy(policy)

    val List(a1, a2, a3, a4, a5, a6) = tickLazyList(ts).take(6).toList

    assert(a1.index == 1)
    assert(a2.index == 2)
    assert(a3.index == 3)
    assert(a4.index == 4)
    assert(a5.index == 5)
    assert(a6.index == 6)

    assert(a1.snooze == 1.second.toJava)
    assert(a2.snooze == 1.second.toJava)
    assert(a3.snooze == 1.second.toJava)
    assert(a4.snooze == 2.seconds.toJava)
    assert(a5.snooze == 2.seconds.toJava)
    assert(a6.snooze == 1.second.toJava)
  }

  test("meet") {
    val policy =
      policies.fixedRate(1.second).meet(policies.fixedDelay(1.seconds))

    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ts: TickStatus = zeroTickStatus.renewPolicy(policy)

    val List(a1, a2, a3, a4, a5, a6) = tickLazyList(ts).take(6).toList

    assert(a1.index == 1)
    assert(a2.index == 2)
    assert(a3.index == 3)
    assert(a4.index == 4)
    assert(a5.index == 5)
    assert(a6.index == 6)

    assert(a1.snooze.toScala <= 1.second)
    assert(a2.snooze.toScala <= 1.second)
    assert(a3.snooze.toScala <= 1.second)
    assert(a4.snooze.toScala <= 1.second)
    assert(a5.snooze.toScala <= 1.second)
    assert(a6.snooze.toScala <= 1.second)
  }

  test("meet - 2") {
    val policy = policies.fixedDelay(1.seconds).meet(policies.fixedRate(1.second))

    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ts: TickStatus = zeroTickStatus.renewPolicy(policy)

    val List(a1, a2, a3, a4, a5, a6) = tickLazyList(ts).take(6).toList

    assert(a1.index == 1)
    assert(a2.index == 2)
    assert(a3.index == 3)
    assert(a4.index == 4)
    assert(a5.index == 5)
    assert(a6.index == 6)

    assert(a1.snooze.toScala <= 1.second)
    assert(a2.snooze.toScala <= 1.second)
    assert(a3.snooze.toScala <= 1.second)
    assert(a4.snooze.toScala <= 1.second)
    assert(a5.snooze.toScala <= 1.second)
    assert(a6.snooze.toScala <= 1.second)
  }

  test("infinite") {
    val policy = policies.fixedRate(1.second).limited(500).repeat
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val loop: Long     = 1000000
    val ts: TickStatus = zeroTickStatus.renewPolicy(policy)
    val tick           = tickLazyList(ts).dropWhile(_.index < loop).take(1).head
    assert(tick.index == loop)
  }

  test("complex policy") {
    val policy = policies
      .accordance(policies.crontab(crontabs.monthly))
      .meet(policies.crontab(crontabs.daily.oneAM))
      .followedBy(policies.crontab(crontabs.daily.twoAM))
      .followedBy(policies.crontab(crontabs.daily.threeAM))
      .followedBy(policies.crontab(crontabs.daily.fourAM).jitter(2.seconds))
      .followedBy(policies.crontab(crontabs.daily.fiveAM))
      .followedBy(policies.crontab(crontabs.daily.sixAM))
      .followedBy(policies.crontab(crontabs.daily.sevenAM))
      .followedBy(policies.crontab(crontabs.daily.eightAM))
      .meet(policies.crontab(crontabs.daily.nineAM))
      .followedBy(policies.crontab(crontabs.daily.tenAM))
      .followedBy(policies.crontab(crontabs.daily.elevenAM))
      .followedBy(policies.giveUp)
      .followedBy(policies.crontab(_.daily.noon))
      .followedBy(policies.crontab(_.daily.onePM))
      .followedBy(policies.crontab(_.daily.twoPM))
      .followedBy(policies.crontab(_.daily.threePM).limited(1))
      .followedBy(policies.crontab(_.daily.fourPM))
      .meet(policies.crontab(_.daily.fivePM))
      .followedBy(policies.crontab(_.daily.sixPM).limited(1).repeat)
      .followedBy(policies.crontab(_.daily.sevenPM))
      .followedBy(policies.crontab(_.daily.eightPM))
      .followedBy(policies.crontab(_.daily.ninePM))
      .followedBy(policies.crontab(_.daily.tenPM))
      .followedBy(policies.crontab(_.daily.elevenPM))
      .followedBy(policies.crontab(_.daily.midnight))
      .repeat
      .except(_.midnight)
      .followedBy(policies.fixedDelay(1.second))
      .followedBy(policies.fixedRate(3.second))
      .followedBy(policies.fixedDelay(1.second, 2.seconds))
      .followedBy(policies.fixedRate(2.seconds))
      .except(_.twoPM)
      .repeat
      .followedBy(policies.crontab(crontabs.weekly.monday))
      .followedBy(policies.crontab(crontabs.weekly.tuesday))
      .followedBy(policies.crontab(crontabs.weekly.wednesday))
      .followedBy(policies.crontab(crontabs.weekly.thursday))
      .followedBy(policies.crontab(crontabs.weekly.friday))
      .followedBy(policies.crontab(crontabs.weekly.saturday))
      .followedBy(policies.crontab(crontabs.weekly.sunday))
      .repeat

    println(policy)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)
  }

  test("decode error") {
    import com.github.chenharryhua.nanjin.common.chrono.*
    assert(decode[Policy](""" {"crontab":"*/4 * * ? *"} """).toOption.isEmpty)
    assert(decode[CronExpr](""" "*/4 * * ? *" """).toOption.isEmpty)
  }

  test("except") {
    val policy = policies.crontab(_.trihourly).except(_.midnight).except(_.elevenPM).except(_.midnight)
    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ticks  = tickLazyList(zeroTickStatus.renewPolicy(policy)).take(8).toList
    val wakeup = ticks.map(_.zonedWakeup.toLocalTime)
    assert(wakeup.size == 8)
    assert(wakeup.head == localTimes.threeAM)
    assert(wakeup(1) == localTimes.sixAM)
    assert(wakeup(2) == localTimes.nineAM)
    assert(wakeup(3) == localTimes.noon)
    assert(wakeup(4) == localTimes.threePM)
    assert(wakeup(5) == localTimes.sixPM)
    assert(wakeup(6) == localTimes.ninePM)
    assert(wakeup(7) == localTimes.threeAM)
  }

  test("offset") {
    val policy = policies.crontab(_.hourly).offset(3.seconds)
    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)
    val ticks  = tickLazyList(zeroTickStatus.renewPolicy(policy)).take(32).toList
    val wakeup = ticks.map(_.zonedWakeup.toLocalTime.getSecond)
    wakeup.forall(_ == 3)
  }

  test("jitter") {
    val policy = policies.crontab(_.hourly).jitter(3.seconds)
    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ts    = zeroTickStatus.renewPolicy(policy)
    val ticks = tickLazyList(ts).take(10).toList

    ticks.foreach(tk => println(tk))
  }
}
