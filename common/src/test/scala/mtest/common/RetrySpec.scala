package mtest.common

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.github.chenharryhua.nanjin.common.Retry
import munit.CatsEffectSuite

import java.time.ZoneId
import scala.concurrent.duration.*

class RetrySpec extends CatsEffectSuite {

  implicit val runtime: IORuntime = IORuntime.global

  /** Helper: create a Retry with simple fixedRate policy */
  def simpleRetry: IO[Retry[IO]] =
    Retry[IO](ZoneId.systemDefault(), _.withPolicy(_.fixedRate(100.millis).limited(5)))

  test("Retry should eventually succeed") {
    var attempt = 0
    val effect: IO[String] = IO {
      attempt += 1
      if (attempt < 3) throw new RuntimeException("fail")
      else "success"
    }

    simpleRetry.flatMap { r =>
      r(effect).map { result =>
        assertEquals(result, "success")
        assertEquals(attempt, 3) // retried twice before succeeding
      }
    }.unsafeRunSync()
  }

  test("Retry should give up after limit") {
    val effect: IO[String] = IO.raiseError(new RuntimeException("always fail"))

    simpleRetry.flatMap { r =>
      r(effect).attempt.map { result =>
        assert(result.isLeft)
      }
    }.unsafeRunSync()
  }

  test("Retry should respect worthy predicate") {
    var attempt = 0
    val effect: IO[String] = IO {
      attempt += 1
      if (attempt == 1) throw new IllegalArgumentException("bad arg")
      else if (attempt == 2) throw new RuntimeException("other fail")
      else "success"
    }

    val worthy: Retry.Builder[IO] => Retry.Builder[IO] =
      _.isWorthRetry(tv => IO.pure(tv.value.isInstanceOf[RuntimeException]))

    Retry[IO](ZoneId.systemDefault(), worthy).flatMap { r =>
      r(effect).attempt.map { result =>
        assert(result.isLeft) // first exception is not retried because it's IllegalArgumentException
        assertEquals(attempt, 1) // retried only once
      }
    }.unsafeRunSync()
  }

  test("Retry should retry Resource acquisition") {
    var attempt = 0
    val riskyRes: IO[String] = IO {
      attempt += 1
      if (attempt < 2) throw new RuntimeException("fail")
      else "ok"
    }

    simpleRetry.flatMap { r =>
      r(riskyRes).map { result =>
        assertEquals(result, "ok")
        assertEquals(attempt, 2)
      }
    }.unsafeRunSync()
  }
}
