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
import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDateTime, LocalTime}
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

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

  test("join") {
    val policy =
      policies.fixedRate(1.second).join(policies.fixedDelay(1.seconds))

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

    assert(a1.snooze == 1.second.toJava)
    assert(a2.snooze == 1.second.toJava)
    assert(a3.snooze == 1.second.toJava)
    assert(a4.snooze == 1.second.toJava)
    assert(a5.snooze == 1.second.toJava)
    assert(a6.snooze == 1.second.toJava)
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
    println(policy)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    tickStream[IO](policy, sydneyTime).debug().take(3).compile.drain.unsafeRunSync()
  }

  test("complex policy") {
    val policy = policies
      .accordance(policies.crontab(crontabs.monthly).endAt(localTimes.midnight))
      .join(policies.crontab(crontabs.daily.amOne).endAt(localTimes.amOne))
      .followedBy(policies.crontab(crontabs.daily.amTwo).endAt(localTimes.amTwo).limited(3))
      .followedBy(policies.crontab(crontabs.daily.amThree).endAt(localTimes.amThree))
      .followedBy(policies.crontab(crontabs.daily.amFour).endAt(localTimes.amFour))
      .followedBy(policies.crontab(crontabs.daily.amFive).endAt(localTimes.amFive))
      .followedBy(policies.crontab(crontabs.daily.amSix).endAt(localTimes.amSix))
      .followedBy(policies.crontab(crontabs.daily.amSeven).endAt(localTimes.amSeven))
      .followedBy(policies.crontab(crontabs.daily.amEight).endAt(localTimes.amEight))
      .join(policies.crontab(crontabs.daily.amNine).endAt(localTimes.amNine))
      .followedBy(policies.crontab(crontabs.daily.amTen).endAt(localTimes.amTen))
      .followedBy(policies.crontab(crontabs.daily.amEleven).endAt(localTimes.amEleven))
      .followedBy(policies.giveUp)
      .followedBy(policies.crontab(_.daily.noon).endAt(localTimes.noon))
      .followedBy(policies.crontab(crontabs.daily.pmOne).endAt(localTimes.pmOne))
      .followedBy(policies.crontab(crontabs.daily.pmTwo).endAt(localTimes.pmTwo))
      .expireAt(LocalDateTime.of(2023, 10, 8, 11, 50, 0))
      .followedBy(policies.crontab(crontabs.daily.pmThree).endAt(localTimes.pmThree).limited(1))
      .followedBy(policies.crontab(crontabs.daily.pmFour).endAt(localTimes.pmFour))
      .join(policies.crontab(crontabs.daily.pmFive).endAt(localTimes.pmFive))
      .followedBy(policies.crontab(crontabs.daily.pmSix).endAt(localTimes.pmSix).limited(1).repeat)
      .followedBy(policies.crontab(crontabs.daily.pmSeven).endAt(localTimes.pmSeven))
      .followedBy(policies.crontab(crontabs.daily.pmEight).endAt(localTimes.pmEight))
      .followedBy(policies.crontab(crontabs.daily.pmNine).endAt(localTimes.pmNine))
      .followedBy(policies.crontab(crontabs.daily.pmTen).endAt(localTimes.pmTen))
      .followedBy(policies.crontab(crontabs.daily.pmEleven).endAt(localTimes.pmEleven))
      .followedBy(policies.crontab(_.daily.midnight).endOfDay)
      .repeat
      .followedBy(policies.fixedDelay(1.second))
      .followedBy(policies.fixedRate(3.second))
      .followedBy(policies.fixedDelay(1.second, 2.seconds))
      .followedBy(policies.fixedRate(2.seconds, 3.second))
      .followedBy(policies.jitter(1.hours, 2.hours))
      .repeat
      .followedBy(policies.crontab(crontabs.weekly.monday).endAt(localTimes.midnight))
      .followedBy(policies.crontab(crontabs.weekly.tuesday).endAt(localTimes.midnight))
      .followedBy(policies.crontab(crontabs.weekly.wednesday).endAt(localTimes.midnight))
      .followedBy(policies.crontab(crontabs.weekly.thursday).endAt(localTimes.midnight))
      .followedBy(policies.crontab(crontabs.weekly.friday).endAt(localTimes.midnight))
      .followedBy(policies.crontab(crontabs.weekly.saturday).endAt(localTimes.midnight))
      .followedBy(policies.crontab(crontabs.weekly.sunday).endAt(localTimes.midnight))

    println(policy)
    println(policy.asJson)
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)
  }

  test("decode error") {
    import com.github.chenharryhua.nanjin.common.chrono.*
    assert(decode[Policy]("""{"a":1}""").toOption.isEmpty)
    assert(decode[CronExpr]("""*/4 * * ? *""").toOption.isEmpty)
  }
}
