package mtest.common

import cats.effect.IO
import munit.CatsEffectSuite
import cats.effect.kernel.Ref
import com.github.chenharryhua.nanjin.common.resilience.Retry
import java.time.ZoneId
import scala.collection.mutable
import scala.concurrent.duration.*

class RetrySpec extends CatsEffectSuite {

  test("1.Retry: effect succeeds after failures") {
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

  test("2.Retry: fails after exhausting policy, only last failure propagated") {
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

  test("3.Retry: decision can stop retry early") {
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

  test("4.Retry: empty policy should not retry") {
    val zoneId = ZoneId.systemDefault()
    var counter = 0

    val riskyOp: IO[String] = IO {
      counter += 1
      throw new RuntimeException(s"fail $counter")
    }

    val retryIO = Retry[IO](zoneId, identity)

    retryIO.flatMap { retry =>
      retry(riskyOp).attempt.map {
        case Left(ex) =>
          assertEquals(ex.getMessage, "fail 1")
          assertEquals(counter, 1)
        case Right(_) => fail("Expected failure")
      }
    }
  }

  test("5.Retry: decision should not be called when effect succeeds immediately") {
    val zoneId = ZoneId.systemDefault()

    val prom = for {
      decisionCalls <- Ref.of[IO, Int](0)
      retry <- Retry[IO](
        zoneId,
        _.withPolicy(_.fixedDelay(20.millis).limited(3)).withDecision { tv =>
          decisionCalls.update(_ + 1).as(tv.map(_ => true))
        })
      result <- retry(IO.pure("ok"))
      calls <- decisionCalls.get
    } yield {
      assertEquals(result, "ok")
      assertEquals(calls, 0)
    }

    prom
  }

  test("6.Retry: decision receives increasing tick indexes") {
    val zoneId = ZoneId.systemDefault()

    val prom = for {
      counter <- Ref.of[IO, Int](0)
      observed <- Ref.of[IO, List[Long]](Nil)
      retry <- Retry[IO](
        zoneId,
        _.withPolicy(_.fixedDelay(10.millis).limited(3)).withDecision { tv =>
          observed.update(_ :+ tv.tick.index).as(tv.map(_ => true))
        })
      _ <- retry(
        counter.updateAndGet(_ + 1).flatMap { n =>
          if (n <= 3) IO.raiseError(new RuntimeException(s"boom $n"))
          else IO.pure("ok")
        }
      )
      indexes <- observed.get
    } yield assertEquals(indexes, List(1L, 2L, 3L))

    prom
  }

  test("7.Retry: decision failure should preserve original operation failure") {
    val zoneId = ZoneId.systemDefault()

    val retryIO = Retry[IO](
      zoneId,
      _.withPolicy(_.fixedDelay(10.millis).limited(1)).withDecision { _ =>
        IO.raiseError(new RuntimeException("decision boom"))
      })

    retryIO.flatMap { retry =>
      retry(IO.raiseError[String](new RuntimeException("operation boom"))).attempt.map {
        case Left(ex) =>
          assertEquals(ex.getMessage, "operation boom")
          assert(ex.getSuppressed.exists(_.getMessage == "decision boom"))
        case Right(_) => fail("Expected failure")
      }
    }
  }
}
