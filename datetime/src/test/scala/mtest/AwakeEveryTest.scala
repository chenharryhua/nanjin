package mtest

import cats.effect.IO
import cats.effect.std.Random
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.datetime.zones.*
import com.github.chenharryhua.nanjin.datetime.{awakeOnPolicy, crontabs, policies, Tick}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.jdk.DurationConverters.JavaDurationOps
import java.time.Duration as JavaDuration

class AwakeEveryTest extends AnyFunSuite {
  test("1.tick") {
    val policy = policies.cronBackoff[IO](crontabs.secondly, saltaTime)
    val ticks  = awakeOnPolicy(policy)

    val res = ticks.map(_.interval.toScala).take(5).compile.toList.unsafeRunSync()
    assert(res.tail.forall(d => d > 0.9.seconds && d < 1.1.seconds), res)
  }

  test("2.process longer than 1 second") {
    val policy = policies.cronBackoff[IO](crontabs.secondly, berlinTime)
    val ticks  = awakeOnPolicy(policy)

    val fds =
      ticks.evalTap(_ => IO.sleep(1.5.seconds)).take(5).compile.toList.unsafeRunSync()
    fds.tail.foreach { t =>
      val interval = t.interval.toScala
      assert(interval > 1.9.seconds)
      assert(interval < 2.1.seconds)
    }
  }

  test("3.process less than 1 second") {
    val policy = policies.cronBackoff[IO](crontabs.secondly, londonTime)
    val ticks  = awakeOnPolicy(policy)

    val fds =
      ticks.evalTap(_ => IO.sleep(0.5.seconds)).take(5).compile.toList.unsafeRunSync()
    fds.tail.foreach { t =>
      val interval = t.interval.toScala
      assert(interval > 0.9.seconds)
      assert(interval < 1.1.seconds)
    }
  }

  test("4.ticks - cron") {
    val policy = policies
      .limitRetriesByCumulativeDelay[IO](5.seconds, policies.cronBackoff[IO](crontabs.secondly, mumbaiTime))
    val ticks = awakeOnPolicy(policy).compile.toList.unsafeRunSync()
    assert(ticks.size === 5, ticks)
    val spend = ticks(4).wakeTime.toEpochMilli / 1000 - ticks.head.wakeTime.toEpochMilli / 1000
    assert(spend === 4, ticks)
  }

  test("5. limitRetriesByDelay") {
    val policy =
      policies.limitRetriesByDelay[IO](1.seconds, policies.cronBackoff[IO](crontabs.hourly, singaporeTime))
    val res: List[Tick] = awakeOnPolicy(policy).compile.toList.unsafeRunSync()
    assert(res.isEmpty)
  }

  test("6. jitters") {
    val policy = policies.cronBackoff[IO](crontabs.secondly, berlinTime)
    val ticks  = awakeOnPolicy(policy)
    val sleep: IO[JavaDuration] =
      Random
        .scalaUtilRandom[IO]
        .flatMap(_.betweenLong(0, 2000))
        .flatMap(d => IO.sleep(d.millisecond).as(JavaDuration.ofMillis(d)))

    val jitters = ticks
      .evalMap(t => sleep.map((t, _)))
      .sliding(2)
      .map { ck =>
        val (_, l1) = ck(0)
        val (t2, _) = ck(1)
        JavaDuration.between(t2.previous.plus(l1).plus(t2.snooze), t2.wakeTime).toMillis
      }
      .take(10)
      .compile
      .toList
      .unsafeRunSync()
    assert(jitters.forall(_ >= 0))
    assert(jitters.forall(_ < 100))
  }
}
