package mtest.common

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.chrono.{crontabs, localTimes, tickStream, Policy}
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
    val List(a1, a2, a3, a4, a5) =
      tickStream.testPolicy[IO]((_: Policy.type) => policy).take(16).compile.toList.unsafeRunSync()
    assert(a1.index == 1)
    assert(a2.index == 2)
    assert(a3.index == 3)
    assert(a4.index == 4)
    assert(a5.index == 5)
    assert(a1.snooze == 1.second.toJava)
    assert(a2.snooze == 1.second.toJava)
    assert(a3.snooze == 1.second.toJava)
    assert(a4.snooze == 2.seconds.toJava)
    assert(a5.snooze == 2.seconds.toJava)
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

    val List(a1, a2, a3, a4, a5, a6) =
      tickStream.testPolicy[IO]((_: Policy.type) => policy).take(6).compile.toList.unsafeRunSync()

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
    assert(List(a1, a2, a3, a4, a5, a6).forall(t => t.acquires.plus(t.snooze) == t.conclude))
  }

  test("meet") {
    val policy =
      Policy.fixedRate(1.second).meet(Policy.fixedDelay(1.seconds))

    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val List(a1, a2, a3, a4, a5, a6) =
      tickStream.testPolicy[IO]((_: Policy.type) => policy).take(6).compile.toList.unsafeRunSync()

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
    assert(List(a1, a2, a3, a4, a5, a6).forall(t => t.acquires.plus(t.snooze) == t.conclude))
  }

  test("meet - 2") {
    val policy = Policy.fixedDelay(1.seconds).meet(Policy.fixedRate(1.second))

    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val List(a1, a2, a3, a4, a5, a6) =
      tickStream.testPolicy[IO]((_: Policy.type) => policy).take(6).compile.toList.unsafeRunSync()

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

    assert(List(a1, a2, a3, a4, a5, a6).forall(t => t.acquires.plus(t.snooze) == t.conclude))
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

    val wakeup = tickStream
      .testPolicy[IO]((_: Policy.type) => policy)
      .take(32)
      .debug()
      .compile
      .toList
      .unsafeRunSync()
      .map(_.local(_.conclude).toLocalTime)
      .distinct
      .sorted

    assert(wakeup.size == 22)
    assert(wakeup.contains(localTimes.oneAM))
    assert(wakeup.contains(localTimes.twoAM))
    assert(wakeup.contains(localTimes.threeAM))
    assert(wakeup.contains(localTimes.fourAM))
    assert(wakeup.contains(localTimes.fiveAM))
    assert(wakeup.contains(localTimes.sixAM))
    assert(wakeup.contains(localTimes.sevenAM))
    assert(wakeup.contains(localTimes.eightAM))
    assert(!wakeup.contains(localTimes.midnight))
    assert(!wakeup.contains(localTimes.elevenPM))
  }

  test("offset") {
    val policy = Policy.crontab(_.hourly).offset(3.seconds)
    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)
    val ticks = tickStream.testPolicy[IO]((_: Policy.type) => policy).take(32).compile.toList.unsafeRunSync()
    assert(ticks.forall(t => t.acquires.plus(t.snooze) == t.conclude))
    val wakeup = ticks.map(_.local(_.conclude).toLocalTime.getSecond)
    wakeup.forall(_ == 3)
  }

  test("jitter") {
    val policy = Policy.crontab(_.hourly).jitter(3.seconds)
    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

  }

  test("limited") {
    val policy = Policy.crontab(_.hourly).limited(3)
    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)
    val ticks = tickStream.testPolicy[IO]((_: Policy.type) => policy).take(60).compile.toList.unsafeRunSync()
    assert(ticks.size == 3)
  }

  test("limited 0") {
    val policy = Policy.crontab(_.hourly).limited(0)
    val ticks = tickStream.testPolicy[IO]((_: Policy.type) => policy).take(6).compile.toList.unsafeRunSync()
    assert(ticks.isEmpty)
  }

  test("limited neg") {
    val policy = Policy.crontab(_.hourly).limited(-1)
    val ticks = tickStream.testPolicy[IO]((_: Policy.type) => policy).take(6).compile.toList.unsafeRunSync()
    assert(ticks.isEmpty)
  }
}
