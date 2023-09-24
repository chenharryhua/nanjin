package mtest.common

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.chrono.zones.{darwinTime, singaporeTime, sydneyTime}
import com.github.chenharryhua.nanjin.common.chrono.{crontabs, policies, tickStream, TickStatus}
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalTime
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

class PolicyCombinatorTest extends AnyFunSuite {

  test("follow by") {
    val policy = policies
      .accordance(policies.fixedDelay(1.second).limited(3))
      .followedBy(policies.fixedDelay(2.seconds).limited(2))
    println(policy.show)
    val ts = TickStatus[IO](policy, singaporeTime).unsafeRunSync()
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
    val ts = TickStatus[IO](policy, darwinTime).unsafeRunSync()
    val t0 = ts.tick.launchTime
    val a1 = ts.next(t0).get
    val a2 = a1.next(t0).get
    val a3 = a2.next(t0).get
    val a4 = a3.next(t0).get
    val a5 = a4.next(t0).get
    val a6 = a5.next(t0).get

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

  ignore("endup") {
    val time = LocalTime.of(16, 55, 0)
    val policy = policies
      .accordance(policies.crontab(crontabs.every10Seconds).endUp(time))
      .followedBy(policies.fixedRate(1.second).endUp(time.plus(5.seconds.toJava)))
      .followedBy(policies.fixedDelay(7.second).endOfDay)
      .repeat
    println(policy)
    tickStream[IO](policy, sydneyTime).debug().compile.drain.unsafeRunSync()
  }
}
