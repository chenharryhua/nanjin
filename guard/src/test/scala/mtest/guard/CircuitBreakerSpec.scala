package mtest.guard

import cats.effect.*
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.action.CircuitBreaker
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
    CircuitBreaker[IO](zoneId)(
      _.withMaxFailures(maxFailures).withPolicy(policy)
    )

  "CircuitBreaker" - {

    "allows successful effects" in
      breaker(1, Policy.giveUp).use { cb =>
        cb.protect(IO.pure(42)).map { result =>
          result shouldBe 42
        }
      }.unsafeRunSync()

    "opens after reaching maxFailures" in {
      val err = new RuntimeException("boom")

      breaker(2, Policy.giveUp).use { cb =>
        for {
          _ <- cb.attempt(IO.raiseError(err))
          _ <- cb.attempt(IO.raiseError(err))
          r <- cb.attempt(IO.unit)
        } yield r.swap.toOption.get shouldBe CircuitBreaker.RejectedException
      }.unsafeRunSync()
    }

    "rejects immediately when open" in {
      val err = new RuntimeException("fail")

      breaker(1, Policy.giveUp).use { cb =>
        for {
          _ <- cb.attempt(IO.raiseError(err))
          r <- cb.attempt(IO.unit)
        } yield r.swap.toOption.get shouldBe CircuitBreaker.RejectedException
      }.unsafeRunSync()
    }

    "moves to half-open after policy tick" in {
      val err = new RuntimeException("fail")

      breaker(
        maxFailures = 1,
        policy = Policy.fixedDelay(100.millis)
      ).use { cb =>
        for {
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
          _ <- IO.sleep(150.millis)

          f1 <- cb.attempt(IO.sleep(50.millis)).start
          f2 <- cb.attempt(IO.unit).start

          r1 <- f1.joinWithNever
          r2 <- f2.joinWithNever
        } yield {
          val results = List(r1, r2)
          results.count(_.isRight) shouldBe 1
          results.count(_.left.exists(_ == CircuitBreaker.RejectedException)) shouldBe 1
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
          _ <- IO.sleep(150.millis)
          _ <- cb.attempt(IO.raiseError(err)) // failed probe
          r <- cb.attempt(IO.unit)
        } yield r.swap.toOption.get shouldBe CircuitBreaker.RejectedException
      }.unsafeRunSync()
    }
  }
}
