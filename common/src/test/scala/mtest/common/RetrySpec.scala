package mtest.common

import cats.effect.{IO, Resource}
import com.github.chenharryhua.nanjin.common.Retry
import munit.CatsEffectSuite

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

  test("Retry: resource acquisition retries on failure") {
    val zoneId = ZoneId.systemDefault()
    val maxAttempts = 3
    val state = mutable.ListBuffer.empty[String]

    var counter = 0
    val riskyResource: Resource[IO, Int] = Resource.make {
      IO {
        counter += 1
        state += s"acquire $counter"
        if (counter < 3) throw new RuntimeException(s"fail $counter")
        else counter
      }
    }(_ => IO(state += s"release $counter") >> IO.println("released"))

    val retryIO = Retry[IO](zoneId, _.withPolicy(_.fixedDelay(10.millis).limited(maxAttempts)))

    retryIO.flatMap { retry =>
      retry(riskyResource).use { r =>
        IO {
          assertEquals(r, 3)
          assertEquals(
            state.toList,
            List(
              "acquire 1",
              "acquire 2",
              "acquire 3"
            )
          )
        }
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
