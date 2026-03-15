package mtest.common

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.chrono.{*, given}
import cron4s.CronExpr
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

class PolicyCombinatorTest extends AnyFunSuite {

  test("simple followed by") {
    val policy = Policy.empty.followedBy(_.fixedDelay(1.second))
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
      tickStream.testPolicy[IO](_.fresh(policy)).take(16).compile.toList.unsafeRunSync()
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
        .followedBy(_.fixedDelay(2.seconds).limited(2))
        .repeat

    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val List(a1, a2, a3, a4, a5, a6) =
      tickStream.testPolicy[IO](_.fresh(policy)).take(6).compile.toList.unsafeRunSync()

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
      Policy.fixedRate(1.second).meet(_.fixedDelay(1.seconds))

    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val List(a1, a2, a3, a4, a5, a6) =
      tickStream.testPolicy[IO](_ => policy).take(6).compile.toList.unsafeRunSync()

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
    val policy = Policy.fixedDelay(1.seconds).meet(_.fresh(Policy.fixedRate(1.second)))

    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val List(a1, a2, a3, a4, a5, a6) =
      tickStream.testPolicy[IO](_.fresh(policy)).take(6).compile.toList.unsafeRunSync()

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
      .crontab(_.monthly)
      .meet(_.crontab(_.daily.oneAM))
      .followedBy(Policy.crontab(crontabs.daily.twoAM))
      .followedBy(Policy.crontab(crontabs.daily.threeAM))
      .followedBy(Policy.crontab(_.daily.fourAM).jitter(2.seconds))
      .followedBy(_.crontab(crontabs.daily.fiveAM))
      .followedBy(_.crontab(_.daily.sixAM))
      .followedBy(_.crontab(_.daily.sevenAM))
      .followedBy(_.crontab(_.daily.eightAM))
      .meet(_.crontab(_.daily.nineAM))
      .followedBy(_.crontab(_.daily.tenAM))
      .followedBy(_.crontab(_.daily.elevenAM))
      .followedBy(_.empty)
      .followedBy(_.crontab(_.daily.noon))
      .followedBy(_.crontab(_.daily.onePM))
      .followedBy(_.crontab(_.daily.twoPM))
      .followedBy(_.crontab(_.daily.threePM).limited(1))
      .followedBy(_.crontab(_.daily.fourPM))
      .meet(_.crontab(_.daily.fivePM))
      .followedBy(_.crontab(_.daily.sixPM).limited(1).repeat)
      .followedBy(_.crontab(_.daily.sevenPM))
      .followedBy(_.crontab(_.daily.eightPM))
      .followedBy(_.crontab(_.daily.ninePM))
      .followedBy(_.crontab(_.daily.tenPM))
      .followedBy(_.crontab(_.daily.elevenPM))
      .followedBy(_.crontab(_.daily.midnight))
      .repeat
      .except(_.midnight)
      .followedBy(_.fixedDelay(1.second))
      .followedBy(_.fixedRate(3.second))
      .followedBy(_.fixedDelay(1.second, 2.seconds))
      .followedBy(_.fixedRate(2.seconds))
      .except(_.twoPM)
      .repeat
      .followedBy(_.crontab(_.weekly.monday))
      .followedBy(_.crontab(_.weekly.tuesday))
      .followedBy(_.crontab(_.weekly.wednesday))
      .followedBy(_.crontab(_.weekly.thursday))
      .followedBy(_.crontab(_.weekly.friday))
      .followedBy(_.crontab(_.weekly.saturday))
      .followedBy(_.crontab(_.weekly.sunday))
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
