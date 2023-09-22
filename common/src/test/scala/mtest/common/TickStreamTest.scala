package mtest.common

import cats.effect.IO
import cats.effect.std.Random
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.*
import com.github.chenharryhua.nanjin.common.chrono.{crontabs, policies, tickStream}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration as JavaDuration
import scala.concurrent.duration.DurationDouble
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

class TickStreamTest extends AnyFunSuite {
  test("1.tick") {
    val policy = policies.crontab(crontabs.secondly).limited(5)
    val ticks  = tickStream[IO](policy, londonTime)

    val res = ticks.map(_.interval.toScala).compile.toList.unsafeRunSync()
    assert(res.tail.forall(d => d === 1.seconds), res)
  }

  test("2.process longer than 1 second") {
    val policy = policies.crontab(crontabs.secondly)
    val ticks  = tickStream[IO](policy, berlinTime)

    val fds =
      ticks.evalTap(_ => IO.sleep(1.5.seconds)).take(5).compile.toList.unsafeRunSync()
    fds.tail.foreach { t =>
      val interval = t.interval.toScala
      assert(interval === 2.seconds)
    }
  }

  test("3.process less than 1 second") {
    val policy = policies.crontab(crontabs.secondly)
    val ticks  = tickStream[IO](policy, cairoTime)

    val fds =
      ticks.evalTap(_ => IO.sleep(0.5.seconds)).take(5).compile.toList.unsafeRunSync()
    fds.tail.foreach { t =>
      val interval = t.interval.toScala
      assert(interval === 1.seconds)
    }
  }

  test("4.constant") {
    val policy = policies.constant(1.second.toJava).limited(5)
    val ticks  = tickStream[IO](policy, saltaTime)
    val sleep: IO[JavaDuration] =
      Random
        .scalaUtilRandom[IO]
        .flatMap(_.betweenLong(0, 500))
        .flatMap(d => IO.sleep(d.toDouble.millisecond).as(JavaDuration.ofMillis(d)))

    ticks.evalTap(_ => sleep).debug().compile.toList.unsafeRunSync()
  }
  test("5.fixed pace") {
    val policy = policies.fixedPace(2.second.toJava).limited(5)
    val ticks  = tickStream[IO](policy, darwinTime)
    val sleep: IO[JavaDuration] =
      Random
        .scalaUtilRandom[IO]
        .flatMap(_.betweenLong(0, 2500))
        .flatMap(d => IO.sleep(d.toDouble.millisecond).as(JavaDuration.ofMillis(d)))

    ticks.evalTap(_ => sleep).map(_.wakeup).debug().compile.toList.unsafeRunSync()
  }

}
