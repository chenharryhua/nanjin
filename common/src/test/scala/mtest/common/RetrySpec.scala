package mtest.common

import cats.effect.IO
import munit.CatsEffectSuite
import cats.syntax.functor.toFunctorOps
import com.github.chenharryhua.nanjin.common.resilience.Retry
import java.time.ZoneId
import scala.collection.mutable
import scala.concurrent.duration.*

class RetrySpec extends CatsEffectSuite {

  test("Retry: effect succeeds after failures") {
    val zoneId = ZoneId.systemDefault()
    val maxAttempts = 3
    val state = mutable.ListBuffer.empty[String]

    // fail first 2 times, succeed 3rd
    var counter = 0
    val riskyOp: IO[String] = IO {
      counter += 1
      state += s"attempt $counter"
      if (counter < 3) throw new RuntimeException(s"fail $counter")
      else "success"
    }

    val retryIO = Retry[IO](zoneId, _.withPolicy(_.fixedDelay(100.millis).limited(maxAttempts)))

    retryIO.flatMap { retry =>
      retry(riskyOp).map { result =>
        assertEquals(result, "success")
        assertEquals(state.toList, List("attempt 1", "attempt 2", "attempt 3"))
      }
    }
  }

  test("Retry: fails after exhausting policy, only last failure propagated") {
    val zoneId = ZoneId.systemDefault()
    val maxAttempts = 2
    var counter = 0

    val riskyOp: IO[String] = IO {
      counter += 1
      throw new RuntimeException(s"fail $counter")
    }

    val retryIO = Retry[IO](zoneId, _.withPolicy(_.fixedDelay(50.millis).limited(maxAttempts)))

    retryIO.flatMap { retry =>
      retry(riskyOp).attempt.map {
        case Left(ex) =>
          assertEquals(ex.getMessage, "fail 3") // only last failure
          assertEquals(counter, 3)
        case Right(_) => fail("Expected failure, got success")
      }
    }
  }

  test("Retry: decision can stop retry early") {
    val zoneId = ZoneId.systemDefault()
    var counter = 0
    val riskyOp: IO[String] = IO {
      counter += 1
      throw new RuntimeException(s"fail $counter")
    }

    val retryIO = Retry[IO](
      zoneId,
      _.withDecision { tv =>
        // Stop retrying after first failure
        IO.pure(tv.map(_ => false))
      })

    retryIO.flatMap { retry =>
      retry(riskyOp).attempt.map {
        case Left(ex) =>
          assertEquals(ex.getMessage, "fail 1")
          assertEquals(counter, 1)
        case Right(_) => fail("Expected failure")
      }
    }
  }
}
