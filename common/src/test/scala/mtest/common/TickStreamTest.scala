package mtest.common

import cats.effect.IO
import cats.effect.std.Random
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.zones.*
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy}
import munit.CatsEffectSuite

import java.time.Duration as JavaDuration
import scala.concurrent.duration.DurationDouble
import scala.jdk.DurationConverters.JavaDurationOps

class TickStreamTest extends CatsEffectSuite {
  test("1.tick") {
    val ticks = tickStream.tickScheduled[IO](londonTime, _.crontab(_.secondly).repeat.limited(5))

    ticks.map(_.window.toScala).compile.toList.map { res =>
      assert(res.tail.forall(d => d === 1.seconds), res)
      assert(res.size == 5)
    }
  }

  test("2.process longer than 1 second") {
    val ticks = tickStream.tickScheduled[IO](berlinTime, _.crontab(_.secondly).repeat)

    ticks.evalTap(_ => IO.sleep(1.5.seconds)).take(5).compile.toList.map { fds =>
      fds.tail.foreach { t =>
        val interval = t.window.toScala
        assert(interval === 2.seconds)
      }
    }
  }

  test("3.process less than 1 second") {
    val ticks = tickStream.tickScheduled[IO](cairoTime, _.crontab(_.secondly).repeat)

    ticks.evalTap(_ => IO.sleep(0.5.seconds)).take(5).compile.toList.map { fds =>
      fds.tail.foreach { t =>
        val interval = t.window.toScala
        assert(interval === 1.seconds)
      }
    }
  }

  test("4.constant") {
    val policy = Policy.fixedDelay(1.second).repeat.limited(5)
    val ticks = tickStream.tickScheduled[IO](saltaTime, (_: Policy.type) => policy)
    val sleep: IO[JavaDuration] =
      Random
        .scalaUtilRandom[IO]
        .flatMap(_.betweenLong(0, 500))
        .flatMap(d => IO.sleep(d.toDouble.millisecond).as(JavaDuration.ofMillis(d)))

    ticks.evalTap(_ => sleep).compile.drain
  }
  test("5.fixed rate") {
    val policy = Policy.fixedRate(2.second).repeat.limited(5)
    val ticks = tickStream.tickScheduled[IO](darwinTime, (_: Policy.type) => policy)
    val sleep: IO[JavaDuration] =
      Random
        .scalaUtilRandom[IO]
        .flatMap(_.betweenLong(0, 2500))
        .flatMap(d => IO.sleep(d.toDouble.millisecond).as(JavaDuration.ofMillis(d)))

    ticks.evalTap(_ => sleep).compile.drain
  }

  test("6.tickImmediate - fixed delay") {
    tickStream
      .tickFuture[IO](saltaTime, _.fixedDelay(1.seconds).repeat.limited(3))
      .compile
      .toList
      .map { ticks =>
        assertEquals(ticks.size, 3)
        val List(a, b, c) = ticks: @unchecked
        assert(a.index == 1)
        assert(b.index == 2)
        assert(c.index == 3)

        assert(a.sequenceId == b.sequenceId)
        assert(b.sequenceId == c.sequenceId)
      }
  }

}
