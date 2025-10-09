package mtest.common

import cats.effect.IO
import cats.effect.std.Random
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.*
import com.github.chenharryhua.nanjin.common.chrono.{crontabs, tickStream, Policy}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration as JavaDuration
import scala.concurrent.duration.DurationDouble
import scala.jdk.DurationConverters.JavaDurationOps

class TickStreamTest extends AnyFunSuite {
  test("1.tick") {
    val policy = Policy.crontab(crontabs.secondly).limited(5)
    val ticks = tickStream.tickScheduled[IO](londonTime, policy)

    val res = ticks.map(_.window.toScala).compile.toList.unsafeRunSync()
    assert(res.tail.forall(d => d === 1.seconds), res)
    assert(res.size == 5)
  }

  test("2.process longer than 1 second") {
    val policy = Policy.crontab(crontabs.secondly)
    val ticks = tickStream.tickScheduled[IO](berlinTime, policy)

    val fds =
      ticks.evalTap(_ => IO.sleep(1.5.seconds)).take(5).compile.toList.unsafeRunSync()
    fds.tail.foreach { t =>
      val interval = t.window.toScala
      assert(interval === 2.seconds)
    }
  }

  test("3.process less than 1 second") {
    val policy = Policy.crontab(crontabs.secondly)
    val ticks = tickStream.tickScheduled[IO](cairoTime, policy)

    val fds =
      ticks.evalTap(_ => IO.sleep(0.5.seconds)).take(5).compile.toList.unsafeRunSync()
    fds.tail.foreach { t =>
      val interval = t.window.toScala
      assert(interval === 1.seconds)
    }
  }

  test("4.constant") {
    val policy = Policy.fixedDelay(1.second).limited(5)
    val ticks = tickStream.tickScheduled[IO](saltaTime, policy)
    val sleep: IO[JavaDuration] =
      Random
        .scalaUtilRandom[IO]
        .flatMap(_.betweenLong(0, 500))
        .flatMap(d => IO.sleep(d.toDouble.millisecond).as(JavaDuration.ofMillis(d)))

    ticks.evalTap(_ => sleep).debug().compile.toList.unsafeRunSync()
  }
  test("5.fixed rate") {
    val policy = Policy.fixedRate(2.second).limited(5)
    val ticks = tickStream.tickScheduled[IO](darwinTime, policy)
    val sleep: IO[JavaDuration] =
      Random
        .scalaUtilRandom[IO]
        .flatMap(_.betweenLong(0, 2500))
        .flatMap(d => IO.sleep(d.toDouble.millisecond).as(JavaDuration.ofMillis(d)))

    ticks.evalTap(_ => sleep).debug().compile.toList.unsafeRunSync()
  }

  test("6.giveUp") {
    val ticks = tickStream.tickScheduled[IO](saltaTime, Policy.giveUp).compile.toList.unsafeRunSync()
    assert(ticks.isEmpty)
  }

  test("7.tickImmediate - giveUp") {
    val ticks = tickStream.tickFuture[IO](saltaTime, Policy.giveUp).compile.toList.unsafeRunSync()
    assert(ticks.isEmpty)
  }

  test("8.tickImmediate - fixed delay") {
    val List(a, b, c) =
      tickStream
        .tickFuture[IO](saltaTime, Policy.fixedDelay(1.seconds).limited(3))
        .compile
        .toList
        .unsafeRunSync()
    assert(a.index == 1)
    assert(b.index == 2)
    assert(c.index == 3)

    assert(a.sequenceId == b.sequenceId)
    assert(b.sequenceId == c.sequenceId)

  }

}
