package mtest.common

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.chrono.{crontabs, localTimes, tickLazyList, Policy, TickStatus}
import cron4s.CronExpr
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

class PolicyCombinatorTest extends AnyFunSuite {

  test("simple followed by") {
    val policy = Policy.giveUp.followedBy(Policy.fixedDelay(1.second))
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)
  }

  test("accordance") {
    val policy = Policy.fixedDelay(1.second)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)
  }

  test("follow by") {
    val policy =
      Policy.fixedDelay(1.second).limited(3).followedBy(Policy.fixedDelay(2.seconds).limited(2))

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
      Policy
        .fixedDelay(1.second)
        .limited(1)
        .repeat
        .limited(3)
        .followedBy(Policy.fixedDelay(2.seconds).limited(2))
        .repeat

    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ts: TickStatus = zeroTickStatus.renewPolicy(policy)

    val List(a1, a2, a3, a4, a5, a6) = tickLazyList.fromTickStatus(ts).take(6).toList

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
      Policy.fixedRate(1.second).meet(Policy.fixedDelay(1.seconds))

    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ts: TickStatus = zeroTickStatus.renewPolicy(policy)

    val List(a1, a2, a3, a4, a5, a6) = tickLazyList.fromTickStatus(ts).take(6).toList

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
    val policy = Policy.fixedDelay(1.seconds).meet(Policy.fixedRate(1.second))

    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ts: TickStatus = zeroTickStatus.renewPolicy(policy)

    val List(a1, a2, a3, a4, a5, a6) = tickLazyList.fromTickStatus(ts).take(6).toList

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
    val policy = Policy.fixedRate(1.second).limited(500).repeat
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val loop: Long = 1000000
    val ts: TickStatus = zeroTickStatus.renewPolicy(policy)
    val tick = tickLazyList.fromTickStatus(ts).dropWhile(_.index < loop).take(1).head
    assert(tick.index == loop)
  }

  test("complex policy") {
    val policy = Policy
      .crontab(crontabs.monthly)
      .meet(Policy.crontab(crontabs.daily.oneAM))
      .followedBy(Policy.crontab(crontabs.daily.twoAM))
      .followedBy(Policy.crontab(crontabs.daily.threeAM))
      .followedBy(Policy.crontab(crontabs.daily.fourAM).jitter(2.seconds))
      .followedBy(Policy.crontab(crontabs.daily.fiveAM))
      .followedBy(Policy.crontab(crontabs.daily.sixAM))
      .followedBy(Policy.crontab(crontabs.daily.sevenAM))
      .followedBy(Policy.crontab(crontabs.daily.eightAM))
      .meet(Policy.crontab(crontabs.daily.nineAM))
      .followedBy(Policy.crontab(crontabs.daily.tenAM))
      .followedBy(Policy.crontab(crontabs.daily.elevenAM))
      .followedBy(Policy.giveUp)
      .followedBy(Policy.crontab(_.daily.noon))
      .followedBy(Policy.crontab(_.daily.onePM))
      .followedBy(Policy.crontab(_.daily.twoPM))
      .followedBy(Policy.crontab(_.daily.threePM).limited(1))
      .followedBy(Policy.crontab(_.daily.fourPM))
      .meet(Policy.crontab(_.daily.fivePM))
      .followedBy(Policy.crontab(_.daily.sixPM).limited(1).repeat)
      .followedBy(Policy.crontab(_.daily.sevenPM))
      .followedBy(Policy.crontab(_.daily.eightPM))
      .followedBy(Policy.crontab(_.daily.ninePM))
      .followedBy(Policy.crontab(_.daily.tenPM))
      .followedBy(Policy.crontab(_.daily.elevenPM))
      .followedBy(Policy.crontab(_.daily.midnight))
      .repeat
      .except(_.midnight)
      .followedBy(Policy.fixedDelay(1.second))
      .followedBy(Policy.fixedRate(3.second))
      .followedBy(Policy.fixedDelay(1.second, 2.seconds))
      .followedBy(Policy.fixedRate(2.seconds))
      .except(_.twoPM)
      .repeat
      .followedBy(Policy.crontab(crontabs.weekly.monday))
      .followedBy(Policy.crontab(crontabs.weekly.tuesday))
      .followedBy(Policy.crontab(crontabs.weekly.wednesday))
      .followedBy(Policy.crontab(crontabs.weekly.thursday))
      .followedBy(Policy.crontab(crontabs.weekly.friday))
      .followedBy(Policy.crontab(crontabs.weekly.saturday))
      .followedBy(Policy.crontab(crontabs.weekly.sunday))
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
    val policy = Policy.crontab(_.hourly).except(_.midnight).except(_.elevenPM).except(_.midnight)
    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ticks = tickLazyList.fromTickStatus(zeroTickStatus.renewPolicy(policy)).take(8).toList
    val wakeup = ticks.map(_.zonedWakeup.toLocalTime)
    assert(wakeup.size == 8)
    assert(wakeup.head == localTimes.oneAM)
    assert(wakeup(1) == localTimes.twoAM)
    assert(wakeup(2) == localTimes.threeAM)
    assert(wakeup(3) == localTimes.fourAM)
    assert(wakeup(4) == localTimes.fiveAM)
    assert(wakeup(5) == localTimes.sixAM)
    assert(wakeup(6) == localTimes.sevenAM)
    assert(wakeup(7) == localTimes.eightAM)
  }

  test("offset") {
    val policy = Policy.crontab(_.hourly).offset(3.seconds)
    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)
    val ticks = tickLazyList.fromTickStatus(zeroTickStatus.renewPolicy(policy)).take(32).toList
    val wakeup = ticks.map(_.zonedWakeup.toLocalTime.getSecond)
    wakeup.forall(_ == 3)
  }

  test("jitter") {
    val policy = Policy.crontab(_.hourly).jitter(3.seconds)
    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ts = zeroTickStatus.renewPolicy(policy)
    val ticks = tickLazyList.fromTickStatus(ts).take(10).toList

    ticks.foreach(tk => println(tk))
  }

  test("100 years") {
    val policy = Policy.fixedDelay(36500.days)
    println(policy.show)
    println(policy.asJson)
    val ticks = tickLazyList.fromOne(policy, sydneyTime).take(10).toList
    ticks.foreach(tk => println(tk))
  }
}
