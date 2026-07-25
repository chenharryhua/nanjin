package mtest.common

import cats.effect.*
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.resilience.CircuitBreaker
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZoneId
import scala.concurrent.duration.*

// by ChatGPT
class CircuitBreakerSpec extends AnyFreeSpec with Matchers {

  implicit val runtime: cats.effect.unsafe.IORuntime =
    cats.effect.unsafe.IORuntime.global

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

  "CircuitBreaker" - {

    "allows successful effects" in
      breaker(1, Policy.fixedDelay(10.seconds)).use { cb =>
        cb.protect(IO.pure(42)).map { result =>
          result shouldBe 42
        }
      }.unsafeRunSync()

    "opens after exceeding maxFailures" in {
      val err = new RuntimeException("boom")

      breaker(2, Policy.fixedDelay(10.seconds)).use { cb =>
        for {
          _ <- cb.attempt(IO.raiseError(err))
          _ <- cb.attempt(IO.raiseError(err))
          _ <- cb.attempt(IO.raiseError(err))
          r <- cb.attempt(IO.unit)
        } yield isRejected(r.swap.toOption.get) shouldBe true
      }.unsafeRunSync()
    }

    "rejects immediately when open" in {
      val err = new RuntimeException("fail")

      breaker(1, Policy.fixedDelay(10.seconds)).use { cb =>
        for {
          _ <- cb.attempt(IO.raiseError(err))
          _ <- cb.attempt(IO.raiseError(err))
          r <- cb.attempt(IO.unit)
        } yield isRejected(r.swap.toOption.get) shouldBe true
      }.unsafeRunSync()
    }

    "exposes Open state without counter" in {
      val err = new RuntimeException("fail")

      breaker(1, Policy.fixedDelay(10.seconds)).use { cb =>
        for {
          _ <- cb.attempt(IO.raiseError(err))
          _ <- cb.attempt(IO.raiseError(err))
          state <- cb.getState
        } yield state shouldBe CircuitBreaker.State.Open
      }.unsafeRunSync()
    }

    "moves to half-open after policy tick" in {
      val err = new RuntimeException("fail")

      breaker(
        maxFailures = 1,
        policy = Policy.fixedDelay(100.millis)
      ).use { cb =>
        for {
          _ <- cb.attempt(IO.raiseError(err))
          _ <- cb.attempt(IO.raiseError(err)) // open
          _ <- IO.sleep(150.millis) // wait for tick
          r <- cb.attempt(IO.unit) // probe allowed
        } yield r shouldBe Right(())
      }.unsafeRunSync()
    }

    "allows only one in-flight call in half-open" in {
      val err = new RuntimeException("fail")

      breaker(
        maxFailures = 1,
        policy = Policy.fixedDelay(100.millis)
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
          results.count(_.isRight) shouldBe 1
          results.count(_.left.exists(isRejected)) shouldBe 1
        }
      }.unsafeRunSync()
    }

    "closes after successful half-open probe" in {
      val err = new RuntimeException("fail")

      breaker(
        maxFailures = 1,
        policy = Policy.fixedDelay(100.millis)
      ).use { cb =>
        for {
          _ <- cb.attempt(IO.raiseError(err))
          _ <- cb.attempt(IO.raiseError(err))
          _ <- IO.sleep(150.millis)
          _ <- cb.attempt(IO.unit) // successful probe
          r <- cb.attempt(IO.unit) // should be fully closed
        } yield r shouldBe Right(())
      }.unsafeRunSync()
    }

    "re-opens if half-open probe fails" in {
      val err = new RuntimeException("fail")

      breaker(
        maxFailures = 1,
        policy = Policy.fixedDelay(100.millis)
      ).use { cb =>
        for {
          _ <- cb.attempt(IO.raiseError(err))
          _ <- cb.attempt(IO.raiseError(err))
          _ <- IO.sleep(150.millis)
          _ <- cb.attempt(IO.raiseError(err)) // failed probe
          r <- cb.attempt(IO.unit)
        } yield isRejected(r.swap.toOption.get) shouldBe true
      }.unsafeRunSync()
    }

    "rejects non-positive maxFailures at configuration time" in {
      assertThrows[IllegalArgumentException] {
        breaker(0, Policy.fixedDelay(10.seconds)).use(_ => IO.unit).unsafeRunSync()
      }

      assertThrows[IllegalArgumentException] {
        breaker(-1, Policy.fixedDelay(10.seconds)).use(_ => IO.unit).unsafeRunSync()
      }
    }

    "stays half-open when half-open probe is canceled" in {
      val err = new RuntimeException("fail")

      breaker(
        maxFailures = 1,
        policy = Policy.fixedDelay(100.millis)
      ).use { cb =>
        for {
          _ <- cb.attempt(IO.raiseError(err))
          _ <- IO.sleep(150.millis)
          probe <- cb.attempt(IO.sleep(500.millis)).start
          _ <- IO.sleep(50.millis)
          _ <- probe.cancel
          r <- cb.attempt(IO.unit)
        } yield r shouldBe Right(())
      }.unsafeRunSync()
    }

    "stays closed when a closed-state call is canceled" in
      breaker(
        maxFailures = 1,
        policy = Policy.fixedDelay(100.millis)
      ).use { cb =>
        for {
          run <- cb.attempt(IO.sleep(500.millis)).start
          _ <- IO.sleep(50.millis)
          _ <- run.cancel
          r <- cb.attempt(IO.unit)
        } yield r shouldBe Right(())
      }.unsafeRunSync()

    "reuses singleton rejection throwable" in
      breaker(1, Policy.fixedDelay(10.seconds)).use { cb =>
        for {
          _ <- cb.attempt(IO.raiseError(new RuntimeException("fail")))
          _ <- cb.attempt(IO.raiseError(new RuntimeException("fail")))
          r1 <- cb.attempt(IO.unit)
          r2 <- cb.attempt(IO.unit)
        } yield {
          val e1 = r1.swap.toOption.get
          val e2 = r2.swap.toOption.get
          isRejected(e1) shouldBe true
          isRejected(e2) shouldBe true
          (e1 eq e2) shouldBe true
        }
      }.unsafeRunSync()

    "does not let stale success overwrite newer closed failures" in
      breaker(3, Policy.fixedDelay(10.seconds)).use { cb =>
        for {
          gate <- Deferred[IO, Unit]
          slowSuccess <- cb.attempt(gate.get.as(())).start
          _ <- IO.sleep(30.millis)
          _ <- cb.attempt(IO.raiseError(new RuntimeException("f1")))
          _ <- cb.attempt(IO.raiseError(new RuntimeException("f2")))
          before <- cb.getState
          _ <- gate.complete(())
          _ <- slowSuccess.joinWithNever
          after <- cb.getState
        } yield {
          before shouldBe CircuitBreaker.State.Closed(2)
          after shouldBe CircuitBreaker.State.Closed(2)
        }
      }.unsafeRunSync()

    "does not let stale failure increment newer closed failures" in
      breaker(3, Policy.fixedDelay(10.seconds)).use { cb =>
        for {
          gate <- Deferred[IO, Unit]
          slowFailure <- cb.attempt(gate.get >> IO.raiseError(new RuntimeException("slow"))).start
          _ <- IO.sleep(30.millis)
          _ <- cb.attempt(IO.raiseError(new RuntimeException("f1")))
          before <- cb.getState
          _ <- gate.complete(())
          _ <- slowFailure.joinWithNever
          after <- cb.getState
        } yield {
          before shouldBe CircuitBreaker.State.Closed(1)
          after shouldBe CircuitBreaker.State.Closed(1)
        }
      }.unsafeRunSync()

    "does not let stale failure write after closed count cycles" in
      breaker(2, Policy.fixedDelay(80.millis)).use { cb =>
        for {
          gate <- Deferred[IO, Unit]
          slowFailure <- cb.attempt(gate.get >> IO.raiseError(new RuntimeException("slow"))).start
          _ <- IO.sleep(20.millis)
          _ <- cb.attempt(IO.raiseError(new RuntimeException("f1")))
          _ <- cb.attempt(IO.raiseError(new RuntimeException("f2")))
          _ <- IO.sleep(120.millis)
          _ <- cb.attempt(IO.unit)
          before <- cb.getState
          _ <- gate.complete(())
          _ <- slowFailure.joinWithNever
          after <- cb.getState
        } yield {
          before shouldBe CircuitBreaker.State.Closed(0)
          after shouldBe CircuitBreaker.State.Closed(0)
        }
      }.unsafeRunSync()
  }
}
