package mtest.common

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.common.chrono.zones.*
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.cats.time.instances.duration.*

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

class PolicyBaseTest extends AnyFunSuite {

  test("fixed delay") {
    val policy = policies.fixedDelay(1.second)
    println(policy.show)

    val ts: TickStatus = TickStatus[IO](policy, beijingTime).unsafeRunSync()

    val List(a1, a2, a3, a4, a5) = lazyTickList(ts).take(5).toList

    assert(a1.sequenceId == ts.tick.sequenceId)
    assert(a1.launchTime == ts.tick.launchTime)
    assert(a1.index == 1)
    assert(a1.previous === ts.tick.wakeup)
    assert(a1.snooze == 1.second.toJava)

    assert(a2.sequenceId == ts.tick.sequenceId)
    assert(a2.launchTime == ts.tick.launchTime)
    assert(a2.index == 2)
    assert(a2.previous === a1.wakeup)
    assert(a2.snooze == 1.second.toJava)

    assert(a3.sequenceId == ts.tick.sequenceId)
    assert(a3.launchTime == ts.tick.launchTime)
    assert(a3.index == 3)
    assert(a3.previous === a2.wakeup)
    assert(a3.snooze == 1.second.toJava)

    assert(a4.sequenceId == ts.tick.sequenceId)
    assert(a4.launchTime == ts.tick.launchTime)
    assert(a4.index == 4)
    assert(a4.previous === a3.wakeup)
    assert(a4.snooze == 1.second.toJava)

    assert(a5.sequenceId == ts.tick.sequenceId)
    assert(a5.launchTime == ts.tick.launchTime)
    assert(a5.index == 5)
    assert(a5.previous === a4.wakeup)
    assert(a5.snooze == 1.second.toJava)
  }

  test("fixed rate") {
    val policy = policies.fixedRate(1.second)
    println(policy.show)

    val ts: TickStatus = TickStatus[IO](policy, beijingTime).unsafeRunSync()

    val List(a1, a2, a3, a4, a5) = lazyTickList(ts).take(5).toList

    assert(a1.sequenceId == ts.tick.sequenceId)
    assert(a1.launchTime == ts.tick.launchTime)
    assert(a1.index == 1)
    assert(a1.previous === ts.tick.wakeup)
    assert(a1.snooze < 1.second.toJava)

    assert(a2.sequenceId == ts.tick.sequenceId)
    assert(a2.launchTime == ts.tick.launchTime)
    assert(a2.index == 2)
    assert(a2.previous === a1.wakeup)
    assert(a2.snooze < 1.second.toJava)

    assert(a3.sequenceId == ts.tick.sequenceId)
    assert(a3.launchTime == ts.tick.launchTime)
    assert(a3.index == 3)
    assert(a3.previous === a2.wakeup)
    assert(a3.snooze < 1.second.toJava)

    assert(a4.sequenceId == ts.tick.sequenceId)
    assert(a4.launchTime == ts.tick.launchTime)
    assert(a4.index == 4)
    assert(a4.previous === a3.wakeup)
    assert(a4.snooze < 1.second.toJava)

    assert(a5.sequenceId == ts.tick.sequenceId)
    assert(a5.launchTime == ts.tick.launchTime)
    assert(a5.index == 5)
    assert(a5.previous === a4.wakeup)
    assert(a5.snooze < 1.second.toJava)
  }

  test("jitter") {
    val policy = policies.jitter(1.minute, 2.hour)
    println(policy.show)
    val ts    = TickStatus[IO](policy, newyorkTime).unsafeRunSync()
    val ticks = lazyTickList(ts).take(500).toList

    ticks.foreach { tk =>
      assert(tk.snooze.toScala >= 1.minute)
      assert(tk.snooze.toScala < 2.hour)
    }
  }

  test("fixed delays") {
    val policy = policies.fixedDelay(1.second, 2.seconds, 3.seconds)
    println(policy.show)
    val ts: TickStatus = TickStatus[IO](policy, mumbaiTime).unsafeRunSync()

    val List(a1, a2, a3, a4, a5, a6, a7) = lazyTickList(ts).take(7).toList

    assert(a1.index == 1)
    assert(a2.index == 2)
    assert(a3.index == 3)
    assert(a4.index == 4)
    assert(a5.index == 5)
    assert(a6.index == 6)
    assert(a7.index == 7)

    assert(a1.snooze == 1.second.toJava)
    assert(a2.snooze == 2.second.toJava)
    assert(a3.snooze == 3.second.toJava)
    assert(a4.snooze == 1.second.toJava)
    assert(a5.snooze == 2.second.toJava)
    assert(a6.snooze == 3.second.toJava)
    assert(a7.snooze == 1.second.toJava)
  }

  test("cron") {
    val policy = policies.crontab(crontabs.hourly)
    println(policy.show)
    val ts = TickStatus[IO](policy, beijingTime).unsafeRunSync()

    val a1 = ts.next(ts.tick.launchTime.plus(1.hour.toJava)).get
    val a2 = a1.next(a1.tick.wakeup.plus(30.minutes.toJava)).get
    val a3 = a2.next(a2.tick.wakeup.plus(45.minutes.toJava)).get
    val a4 = a3.next(a2.tick.wakeup.plus(60.minutes.toJava)).get

    assert(a1.tick.index == 1)
    assert(a2.tick.index == 2)
    assert(a3.tick.index == 3)
    assert(a4.tick.index == 4)

    assert(a2.tick.snooze == 30.minutes.toJava)
    assert(a3.tick.snooze == 15.minutes.toJava)
    assert(a4.tick.snooze == 1.hour.toJava)
  }
}
