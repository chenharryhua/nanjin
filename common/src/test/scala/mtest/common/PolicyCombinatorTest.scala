package mtest.common

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.chrono.zones.{darwinTime, singaporeTime, sydneyTime}
import com.github.chenharryhua.nanjin.common.chrono.{
  crontabs,
  localTimes,
  policies,
  tickStream,
  Policy,
  TickStatus
}
import cron4s.CronExpr
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDateTime, LocalTime}
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

    val ts = TickStatus.zeroth[IO](policy, singaporeTime).unsafeRunSync()
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
        .accordance(policies.fixedDelay(1.second).repeat.limited(3))
        .followedBy(policies.fixedDelay(2.seconds).limited(2))
        .repeat

    println(policy.show)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ts: TickStatus = TickStatus.zeroth[IO](policy, darwinTime).unsafeRunSync()

    val List(a1, a2, a3, a4, a5, a6) = lazyTickList(ts).take(6).toList

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

    val ts: TickStatus = TickStatus.zeroth[IO](policy, sydneyTime).unsafeRunSync()

    val List(a1, a2, a3, a4, a5, a6) = lazyTickList(ts).take(6).toList

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
    val ts: TickStatus = TickStatus.zeroth[IO](policy, darwinTime).unsafeRunSync()
    val tick           = lazyTickList(ts).dropWhile(_.index < loop).take(1).head
    assert(tick.index == loop)
  }

  test("end at") {
    val time = LocalTime.of(16, 55, 0)
    val policy = policies
      .accordance(policies.crontab(crontabs.secondly).endAt(time))
      .followedBy(policies.fixedRate(1.second).endAt(time.plus(5.seconds.toJava)))
      .followedBy(policies.fixedDelay(2.second).endOfDay)
      .repeat
      .expireAt(LocalDateTime.MAX)
    println(policy)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    tickStream[IO](policy, sydneyTime).debug().take(3).compile.drain.unsafeRunSync()
  }

  test("end at midnight") {
    val policy = policies.crontab(crontabs.secondly).endAt(_.midnight)
    println(policy)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)
    assert(tickStream[IO](policy, sydneyTime).compile.toList.unsafeRunSync().isEmpty)
  }
  test("expire at") {
    val policy = policies.crontab(crontabs.secondly).expireAt(LocalDateTime.of(2023, 1, 1, 12, 0, 0))
    println(policy)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)
    assert(tickStream[IO](policy, sydneyTime).compile.toList.unsafeRunSync().isEmpty)
  }

  test("complex policy") {
    val policy = policies
      .accordance(policies.crontab(crontabs.monthly).endAt(localTimes.midnight))
      .meet(policies.crontab(crontabs.daily.oneAM).endAt(localTimes.oneAM))
      .followedBy(policies.crontab(crontabs.daily.twoAM).endAt(localTimes.twoAM).limited(3))
      .followedBy(policies.crontab(crontabs.daily.threeAM).endAt(localTimes.threeAM))
      .followedBy(policies.crontab(crontabs.daily.fourAM).endAt(localTimes.fourAM))
      .followedBy(policies.crontab(crontabs.daily.fiveAM).endAt(localTimes.fiveAM))
      .followedBy(policies.crontab(crontabs.daily.sixAM).endAt(localTimes.sixAM))
      .followedBy(policies.crontab(crontabs.daily.sevenAM).endAt(localTimes.sevenAM))
      .followedBy(policies.crontab(crontabs.daily.eightAM).endAt(localTimes.eightAM))
      .meet(policies.crontab(crontabs.daily.nineAM).endAt(localTimes.nineAM))
      .followedBy(policies.crontab(crontabs.daily.tenAM).endAt(localTimes.tenAM))
      .followedBy(policies.crontab(crontabs.daily.elevenAM).endAt(localTimes.elevenAM))
      .followedBy(policies.giveUp)
      .followedBy(policies.crontab(_.daily.noon).endAt(localTimes.noon))
      .followedBy(policies.crontab(crontabs.daily.onePM).endAt(_.onePM))
      .followedBy(policies.crontab(crontabs.daily.twoPM).endAt(_.twoPM))
      .expireAt(LocalDateTime.of(2023, 10, 8, 11, 50, 0))
      .followedBy(policies.crontab(crontabs.daily.threePM).endAt(_.threePM).limited(1))
      .followedBy(policies.crontab(crontabs.daily.fourPM).endAt(_.fourPM))
      .meet(policies.crontab(crontabs.daily.fivePM).endAt(_.fivePM))
      .followedBy(policies.crontab(crontabs.daily.sixPM).endAt(_.sixPM).limited(1).repeat)
      .followedBy(policies.crontab(crontabs.daily.sevenPM).endAt(_.sevenPM))
      .followedBy(policies.crontab(crontabs.daily.eightPM).endAt(_.eightPM))
      .followedBy(policies.crontab(crontabs.daily.ninePM).endAt(_.ninePM))
      .followedBy(policies.crontab(crontabs.daily.tenPM).endAt(_.tenPM))
      .followedBy(policies.crontab(crontabs.daily.elevenPM).endAt(_.elevenPM))
      .followedBy(policies.crontab(_.daily.midnight).endOfDay)
      .repeat
      .followedBy(policies.fixedDelay(1.second))
      .followedBy(policies.fixedRate(3.second))
      .followedBy(policies.fixedDelay(1.second, 2.seconds))
      .followedBy(policies.fixedRate(2.seconds, 3.second))
      .followedBy(policies.jitter(1.hours, 2.hours))
      .repeat
      .followedBy(policies.crontab(crontabs.weekly.monday).endAt(_.oneAM))
      .followedBy(policies.crontab(crontabs.weekly.tuesday).endAt(_.twoAM))
      .followedBy(policies.crontab(crontabs.weekly.wednesday).endAt(_.threeAM))
      .followedBy(policies.crontab(crontabs.weekly.thursday).endAt(_.fourAM))
      .followedBy(policies.crontab(crontabs.weekly.friday).endAt(localTimes.fiveAM))
      .followedBy(policies.crontab(crontabs.weekly.saturday).endAt(localTimes.sixAM))
      .followedBy(policies.crontab(crontabs.weekly.sunday).endAt(localTimes.sevenAM))
      .expireAt(LocalDateTime.MAX)
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
}
