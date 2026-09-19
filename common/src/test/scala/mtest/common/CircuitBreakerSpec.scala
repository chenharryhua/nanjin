package mtest.common

import cats.effect.*
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.resilience.CircuitBreaker
import munit.CatsEffectSuite

import java.time.ZoneId
import scala.concurrent.duration.*

// by ChatGPT
class CircuitBreakerSpec extends CatsEffectSuite {

  private val zoneId = ZoneId.systemDefault()

  private def breaker(
    maxFailures: Int,
    policy: Policy
  ): Resource[IO, CircuitBreaker[IO]] =
    CircuitBreaker[IO](
      zoneId,
      maxFailures,
      _ => policy
    )

  private def isRejected(ex: Throwable): Boolean = ex match {
    case CircuitBreaker.RejectedException =>
      ex.getMessage == "CircuitBreaker rejected"
    case _ => false
  }

  test("CircuitBreaker: allows successful effects") {
    breaker(1, Policy.fixedDelay(10.seconds).repeat).use { cb =>
      cb.protect(IO.pure(42)).map { result =>
        assertEquals(result, 42)
      }
    }
  }

  test("CircuitBreaker: opens after exceeding maxFailures") {
    val err = new RuntimeException("boom")

    breaker(2, Policy.fixedDelay(10.seconds).repeat).use { cb =>
      for {
        _ <- cb.attempt(IO.raiseError(err))
        _ <- cb.attempt(IO.raiseError(err))
        _ <- cb.attempt(IO.raiseError(err))
        r <- cb.attempt(IO.unit)
      } yield assert(isRejected(r.swap.toOption.get))
    }
  }

  test("CircuitBreaker: rejects immediately when open") {
    val err = new RuntimeException("fail")

    breaker(1, Policy.fixedDelay(10.seconds).repeat).use { cb =>
      for {
        _ <- cb.attempt(IO.raiseError(err))
        _ <- cb.attempt(IO.raiseError(err))
        r <- cb.attempt(IO.unit)
      } yield assert(isRejected(r.swap.toOption.get))
    }
  }

  test("CircuitBreaker: exposes Open state without counter") {
    val err = new RuntimeException("fail")

    breaker(1, Policy.fixedDelay(10.seconds).repeat).use { cb =>
      for {
        _ <- cb.attempt(IO.raiseError(err))
        _ <- cb.attempt(IO.raiseError(err))
        state <- cb.state
      } yield assertEquals(state, CircuitBreaker.State.Open)
    }
  }

  test("CircuitBreaker: moves to half-open after policy tick") {
    val err = new RuntimeException("fail")

    breaker(
      maxFailures = 1,
      policy = Policy.fixedDelay(100.millis).repeat
    ).use { cb =>
      for {
        _ <- cb.attempt(IO.raiseError(err))
        _ <- cb.attempt(IO.raiseError(err)) // open
        _ <- IO.sleep(150.millis) // wait for tick
        r <- cb.attempt(IO.unit) // probe allowed
      } yield assertEquals(r, Right(()))
    }
  }

  test("CircuitBreaker: allows only one in-flight call in half-open") {
    val err = new RuntimeException("fail")

    breaker(
      maxFailures = 1,
      policy = Policy.fixedDelay(100.millis).repeat
    ).use { cb =>
      for {
        _ <- cb.attempt(IO.raiseError(err))
        _ <- cb.attempt(IO.raiseError(err))
        _ <- IO.sleep(150.millis)

        f1 <- cb.attempt(IO.sleep(50.millis)).start
        f2 <- cb.attempt(IO.unit).start

        r1 <- f1.joinWithNever
        r2 <- f2.joinWithNever
      } yield {
        val results = List(r1, r2)
        assertEquals(results.count(_.isRight), 1)
        assertEquals(results.count(_.left.exists(isRejected)), 1)
      }
    }
  }

  test("CircuitBreaker: closes after successful half-open probe") {
    val err = new RuntimeException("fail")

    breaker(
      maxFailures = 1,
      policy = Policy.fixedDelay(100.millis).repeat
    ).use { cb =>
      for {
        _ <- cb.attempt(IO.raiseError(err))
        _ <- cb.attempt(IO.raiseError(err))
        _ <- IO.sleep(150.millis)
        _ <- cb.attempt(IO.unit) // successful probe
        r <- cb.attempt(IO.unit) // should be fully closed
      } yield assertEquals(r, Right(()))
    }
  }

  test("CircuitBreaker: re-opens if half-open probe fails") {
    val err = new RuntimeException("fail")

    breaker(
      maxFailures = 1,
      policy = Policy.fixedDelay(100.millis).repeat
    ).use { cb =>
      for {
        _ <- cb.attempt(IO.raiseError(err))
        _ <- cb.attempt(IO.raiseError(err))
        _ <- IO.sleep(150.millis)
        _ <- cb.attempt(IO.raiseError(err)) // failed probe
        r <- cb.attempt(IO.unit)
      } yield assert(isRejected(r.swap.toOption.get))
    }
  }

  test("CircuitBreaker: rejects non-positive maxFailures at configuration time") {
    // maxFailures is validated eagerly while constructing the Resource, so the
    // IllegalArgumentException is thrown synchronously rather than inside the effect.
    intercept[IllegalArgumentException](breaker(0, Policy.fixedDelay(10.seconds).repeat))
    intercept[IllegalArgumentException](breaker(-1, Policy.fixedDelay(10.seconds).repeat))
  }

  test("CircuitBreaker: stays half-open when half-open probe is canceled") {
    val err = new RuntimeException("fail")

    breaker(
      maxFailures = 1,
      policy = Policy.fixedDelay(100.millis).repeat
    ).use { cb =>
      for {
        _ <- cb.attempt(IO.raiseError(err))
        _ <- IO.sleep(150.millis)
        probe <- cb.attempt(IO.sleep(500.millis)).start
        _ <- IO.sleep(50.millis)
        _ <- probe.cancel
        r <- cb.attempt(IO.unit)
      } yield assertEquals(r, Right(()))
    }
  }

  test("CircuitBreaker: stays closed when a closed-state call is canceled") {
    breaker(
      maxFailures = 1,
      policy = Policy.fixedDelay(100.millis).repeat
    ).use { cb =>
      for {
        run <- cb.attempt(IO.sleep(500.millis)).start
        _ <- IO.sleep(50.millis)
        _ <- run.cancel
        r <- cb.attempt(IO.unit)
      } yield assertEquals(r, Right(()))
    }
  }

  test("CircuitBreaker: reuses singleton rejection throwable") {
    breaker(1, Policy.fixedDelay(10.seconds).repeat).use { cb =>
      for {
        _ <- cb.attempt(IO.raiseError(new RuntimeException("fail")))
        _ <- cb.attempt(IO.raiseError(new RuntimeException("fail")))
        r1 <- cb.attempt(IO.unit)
        r2 <- cb.attempt(IO.unit)
      } yield {
        val e1 = r1.swap.toOption.get
        val e2 = r2.swap.toOption.get
        assert(isRejected(e1))
        assert(isRejected(e2))
        assert(e1 eq e2)
      }
    }
  }

  test("CircuitBreaker: does not let stale success overwrite newer closed failures") {
    breaker(3, Policy.fixedDelay(10.seconds).repeat).use { cb =>
      for {
        gate <- Deferred[IO, Unit]
        slowSuccess <- cb.attempt(gate.get.as(())).start
        _ <- IO.sleep(30.millis)
        _ <- cb.attempt(IO.raiseError(new RuntimeException("f1")))
        _ <- cb.attempt(IO.raiseError(new RuntimeException("f2")))
        before <- cb.state
        _ <- gate.complete(())
        _ <- slowSuccess.joinWithNever
        after <- cb.state
      } yield {
        assertEquals(before, CircuitBreaker.State.Closed(2))
        assertEquals(after, CircuitBreaker.State.Closed(2))
      }
    }
  }

  test("CircuitBreaker: does not let stale failure increment newer closed failures") {
    breaker(3, Policy.fixedDelay(10.seconds).repeat).use { cb =>
      for {
        gate <- Deferred[IO, Unit]
        slowFailure <- cb.attempt(gate.get >> IO.raiseError(new RuntimeException("slow"))).start
        _ <- IO.sleep(30.millis)
        _ <- cb.attempt(IO.raiseError(new RuntimeException("f1")))
        before <- cb.state
        _ <- gate.complete(())
        _ <- slowFailure.joinWithNever
        after <- cb.state
      } yield {
        assertEquals(before, CircuitBreaker.State.Closed(1))
        assertEquals(after, CircuitBreaker.State.Closed(1))
      }
    }
  }

  test("CircuitBreaker: does not let stale failure write after closed count cycles") {
    breaker(2, Policy.fixedDelay(80.millis).repeat).use { cb =>
      for {
        gate <- Deferred[IO, Unit]
        slowFailure <- cb.attempt(gate.get >> IO.raiseError(new RuntimeException("slow"))).start
        _ <- IO.sleep(20.millis)
        _ <- cb.attempt(IO.raiseError(new RuntimeException("f1")))
        _ <- cb.attempt(IO.raiseError(new RuntimeException("f2")))
        _ <- IO.sleep(120.millis)
        _ <- cb.attempt(IO.unit)
        before <- cb.state
        _ <- gate.complete(())
        _ <- slowFailure.joinWithNever
        after <- cb.state
      } yield {
        assertEquals(before, CircuitBreaker.State.Closed(0))
        assertEquals(after, CircuitBreaker.State.Closed(0))
      }
    }
  }

  test("CircuitBreaker: State encoder produces correct JSON for Closed") {
    import io.circe.Encoder

    val state: CircuitBreaker.State = CircuitBreaker.State.Closed(3)
    val json = Encoder[CircuitBreaker.State].apply(state)
    assertEquals(json.hcursor.get[String]("state").toOption.get, "Closed")
    assertEquals(json.hcursor.get[Int]("failures").toOption.get, 3)
  }

  test("CircuitBreaker: State encoder produces correct JSON for HalfOpen") {
    import io.circe.Encoder

    val state: CircuitBreaker.State = CircuitBreaker.State.HalfOpen
    val json = Encoder[CircuitBreaker.State].apply(state)
    assertEquals(json.hcursor.get[String]("state").toOption.get, "Half-Open")
  }

  test("CircuitBreaker: State encoder produces correct JSON for Open") {
    import io.circe.Encoder

    val state: CircuitBreaker.State = CircuitBreaker.State.Open
    val json = Encoder[CircuitBreaker.State].apply(state)
    assertEquals(json.hcursor.get[String]("state").toOption.get, "Open")
  }

  test("CircuitBreaker: reports HalfOpen state after cancel restores probe admission") {
    val err = new RuntimeException("fail")

    breaker(
      maxFailures = 1,
      policy = Policy.fixedDelay(100.millis).repeat
    ).use { cb =>
      for {
        _ <- cb.attempt(IO.raiseError(err))
        _ <- cb.attempt(IO.raiseError(err)) // open
        _ <- IO.sleep(150.millis) // tick -> half-open
        state1 <- cb.state
        probe <- cb.attempt(IO.sleep(500.millis)).start
        _ <- IO.sleep(30.millis)
        _ <- probe.cancel // cancel half-open probe
        state2 <- cb.state
      } yield {
        assertEquals(state1, CircuitBreaker.State.HalfOpen)
        assertEquals(state2, CircuitBreaker.State.HalfOpen)
      }
    }
  }

  test("CircuitBreaker: half-open probe success from stale Closed admission is ignored") {
    // This exercises the evolve(HalfOpenRunning, from=Closed) -> ms path
    breaker(
      maxFailures = 2,
      policy = Policy.fixedDelay(80.millis).repeat
    ).use { cb =>
      for {
        gate <- Deferred[IO, Unit]
        // Start a slow success from Closed state
        slowFromClosed <- cb.attempt(gate.get).start
        _ <- IO.sleep(20.millis)
        // Trip to Open
        _ <- cb.attempt(IO.raiseError(new RuntimeException("f1")))
        _ <- cb.attempt(IO.raiseError(new RuntimeException("f2")))
        _ <- IO.sleep(120.millis) // tick -> half-open
        // Start a probe from HalfOpen
        probeFromHalfOpen <- cb.attempt(IO.sleep(200.millis)).start
        _ <- IO.sleep(20.millis)
        // Now release the stale Closed-admission success
        _ <- gate.complete(())
        _ <- slowFromClosed.joinWithNever
        // State should still be HalfOpen (running), not Closed
        state <- cb.state
        _ <- probeFromHalfOpen.cancel
      } yield assertEquals(state, CircuitBreaker.State.HalfOpen)
    }
  }
}
