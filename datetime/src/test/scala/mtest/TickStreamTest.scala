package mtest

import cats.effect.IO
import cats.effect.std.Random
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import com.github.chenharryhua.nanjin.datetime.zones.*
import com.github.chenharryhua.nanjin.datetime.{crontabs, policies, tickStream}
import org.scalatest.funsuite.AnyFunSuite
import shapeless.test.illTyped

import java.time.Duration as JavaDuration
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

class TickStreamTest extends AnyFunSuite {
  test("1.tick") {
    val policy = policies.cronBackoff[IO](crontabs.secondly, saltaTime)
    val ticks  = tickStream(policy)

    val res = ticks.map(_.interval.toScala).take(5).compile.toList.unsafeRunSync()
    assert(res.tail.forall(d => d > 0.9.seconds && d < 1.1.seconds), res)
  }

  test("2.process longer than 1 second") {
    val policy = policies.cronBackoff[IO](crontabs.secondly, berlinTime)
    val ticks  = tickStream(policy)

    val fds =
      ticks.evalTap(_ => IO.sleep(1.5.seconds)).take(5).compile.toList.unsafeRunSync()
    fds.tail.foreach { t =>
      val interval = t.interval.toScala
      assert(interval > 1.8.seconds)
      assert(interval < 2.2.seconds)
    }
  }

  test("3.process less than 1 second") {
    val policy = policies.cronBackoff[IO](crontabs.secondly, londonTime)
    val ticks  = tickStream(policy)

    val fds =
      ticks.evalTap(_ => IO.sleep(0.5.seconds)).take(5).compile.toList.unsafeRunSync()
    fds.tail.foreach { t =>
      val interval = t.interval.toScala
      assert(interval > 0.8.seconds)
      assert(interval < 1.2.seconds)
    }
  }

  test("4.ticks - session id should not be same") {
    val policy = policies
      .limitRetriesByCumulativeDelay[IO](5.seconds, policies.cronBackoff[IO](crontabs.secondly, mumbaiTime))
    val ticks = tickStream(policy).compile.toList.unsafeRunSync()
    assert(ticks.size === 5, ticks)
    val spend = ticks(4).timestamp.toEpochMilli / 1000 - ticks.head.timestamp.toEpochMilli / 1000
    assert(spend === 4, ticks)
    assert(ticks.map(_.streamId).distinct.size == 1)
    assert(ticks.last.predict.isEmpty)
  }

  test("5.limitRetriesByDelay") {
    val policy =
      policies.limitRetriesByDelay[IO](1.seconds, policies.cronBackoff[IO](crontabs.hourly, singaporeTime))
    val res: List[Tick] = tickStream(policy).compile.toList.unsafeRunSync()
    assert(res.isEmpty)
  }

  test("6.cron") {
    val policy = policies.cronBackoff[IO](crontabs.secondly, berlinTime)
    val ticks  = tickStream(policy)
    val sleep: IO[JavaDuration] =
      Random
        .scalaUtilRandom[IO]
        .flatMap(_.betweenLong(0, 500))
        .flatMap(d => IO.sleep(d.millisecond).as(JavaDuration.ofMillis(d)))

    val jitters = ticks
      .evalMap(t => sleep.map((t, _)))
      .sliding(2)
      .map { ck =>
        val (t1, l1) = ck(0)
        val (t2, _)  = ck(1)
        assert(t2.interval === JavaDuration.between(t1.timestamp, t2.timestamp))
        assert(t2.interval.toScala === t2.status.previousDelay.get)
        assert(t2.snoozed.toScala < t2.interval.toScala)
        val dur = JavaDuration
          .between(t2.previous.truncatedTo(ChronoUnit.SECONDS), t2.timestamp.truncatedTo(ChronoUnit.SECONDS))
          .toScala
        assert(dur === 1.second)
        assert(t1.streamId === t2.streamId)
        assert(t2.previous === t1.timestamp)
        assert(t2.previous.plusNanos(t2.status.previousDelay.get.toNanos) === t2.timestamp)
        assert(t1.predict.exists(_.isBefore(t2.timestamp)))
        val j = JavaDuration.between(t2.previous.plus(l1).plus(t2.snoozed), t2.timestamp).toNanos
        assert(j > 0)
        t1
      }
      .take(10)
      .compile
      .toList
      .unsafeRunSync()
    val t1 = jitters.head
    val t5 = jitters(5)
    val t9 = jitters(9)
    assert(t1.previous.plus(t5.status.cumulativeDelay.toJava) == t5.timestamp)
    assert(t1.previous.plus(t9.status.cumulativeDelay.toJava) == t9.timestamp)
  }

  test("7.constant") {
    val policy = policies.constantDelay[IO](1.second)
    val ticks  = tickStream(policy)
    val sleep: IO[JavaDuration] =
      Random
        .scalaUtilRandom[IO]
        .flatMap(_.betweenLong(0, 500))
        .flatMap(d => IO.sleep(d.millisecond).as(JavaDuration.ofMillis(d)))

    val jitters = ticks.evalTap(_ => sleep).take(10).compile.toList.unsafeRunSync()
    val t1      = jitters.head
    val t5      = jitters(5)
    val t9      = jitters(9)
    assert(t1.previous.plus(t5.status.cumulativeDelay.toJava) == t5.timestamp)
    assert(t1.previous.plus(t9.status.cumulativeDelay.toJava) == t9.timestamp)
  }

  test("8.jitter") {
    val policy = policies.jitterBackoff[IO](0.second, 1.seconds)
    val ticks  = tickStream(policy)
    val sleep: IO[JavaDuration] =
      Random
        .scalaUtilRandom[IO]
        .flatMap(_.betweenLong(0, 500))
        .flatMap(d => IO.sleep(d.millisecond).as(JavaDuration.ofMillis(d)))

    val jitters = ticks.evalTap(_ => sleep).take(10).compile.toList.unsafeRunSync()
    val t1      = jitters.head
    val t5      = jitters(5)
    val t9      = jitters(9)
    assert(t1.previous.plus(t5.status.cumulativeDelay.toJava) == t5.timestamp)
    assert(t1.previous.plus(t9.status.cumulativeDelay.toJava) == t9.timestamp)
  }

  test("9.disable copy and new of Tick") {
    val t = Tick.Zero[IO].unsafeRunSync()
    assert(t.index === 0)
    illTyped(""" t.copy(snooze = JavaDuration.ZERO) """)

    illTyped(""" new Tick(t.sessionId, t.snooze, t.previous, t.wakeTime, t.status) """)
  }
}
