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
    val ticks  = tickStream.fromOne[IO](policy, londonTime)

    val res = ticks.map(_.interval.toScala).compile.toList.unsafeRunSync()
    assert(res.tail.forall(d => d === 1.seconds), res)
    assert(res.size == 5)
  }

  test("2.process longer than 1 second") {
    val policy = Policy.crontab(crontabs.secondly)
    val ticks  = tickStream.fromOne[IO](policy, berlinTime)

    val fds =
      ticks.evalTap(_ => IO.sleep(1.5.seconds)).take(5).compile.toList.unsafeRunSync()
    fds.tail.foreach { t =>
      val interval = t.interval.toScala
      assert(interval === 2.seconds)
    }
  }

  test("3.process less than 1 second") {
    val policy = Policy.crontab(crontabs.secondly)
    val ticks  = tickStream.fromOne[IO](policy, cairoTime)

    val fds =
      ticks.evalTap(_ => IO.sleep(0.5.seconds)).take(5).compile.toList.unsafeRunSync()
    fds.tail.foreach { t =>
      val interval = t.interval.toScala
      assert(interval === 1.seconds)
    }
  }

  test("4.constant") {
    val policy                  = Policy.fixedDelay(1.second).limited(5)
    val ticks                   = tickStream.fromOne[IO](policy, saltaTime)
    val sleep: IO[JavaDuration] =
      Random
        .scalaUtilRandom[IO]
        .flatMap(_.betweenLong(0, 500))
        .flatMap(d => IO.sleep(d.toDouble.millisecond).as(JavaDuration.ofMillis(d)))

    ticks.evalTap(_ => sleep).debug().compile.toList.unsafeRunSync()
  }
  test("5.fixed rate") {
    val policy                  = Policy.fixedRate(2.second).limited(5)
    val ticks                   = tickStream.fromOne[IO](policy, darwinTime)
    val sleep: IO[JavaDuration] =
      Random
        .scalaUtilRandom[IO]
        .flatMap(_.betweenLong(0, 2500))
        .flatMap(d => IO.sleep(d.toDouble.millisecond).as(JavaDuration.ofMillis(d)))

    ticks.evalTap(_ => sleep).debug().compile.toList.unsafeRunSync()
  }

  test("6.giveUp") {
    val ticks = tickStream.fromOne[IO](Policy.giveUp, saltaTime).compile.toList.unsafeRunSync()
    assert(ticks.isEmpty)
  }

  test("7.from zero - nonEmpty") {
    val ticks = tickStream.fromZero[IO](Policy.giveUp, saltaTime).compile.toList.unsafeRunSync()
    assert(ticks.head.index == 0)
  }

  test("8.from zero") {
    val List(a, b, c) = tickStream
      .fromZero[IO](Policy.fixedDelay(1.seconds).limited(2), saltaTime)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.index == 0)
    assert(b.index == 1)
    assert(c.index == 2)

    assert(a.sequenceId == b.sequenceId)
    assert(b.sequenceId == c.sequenceId)

  }

}
