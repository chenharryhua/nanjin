package com.github.chenharryhua.nanjin.common.resilience

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.Policy
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

class CircuitBreakerInternalSpec extends AnyFreeSpec with Matchers {

  implicit val runtime: cats.effect.unsafe.IORuntime =
    cats.effect.unsafe.IORuntime.global

  "CircuitBreaker rejection classification" - {

    "maps rejection to singleton RejectedException" in {
      val ex = CircuitBreaker[IO](
        ZoneId.systemDefault(),
        maxFailures = 1,
        _ => Policy.fixedDelay(10.seconds)
      ).use { cb =>
        for {
          _ <- cb.attempt(IO.raiseError(new RuntimeException("fail")))
          _ <- cb.attempt(IO.raiseError(new RuntimeException("fail")))
          rejected <- cb.attempt(IO.unit)
        } yield rejected.swap.toOption.get
      }.unsafeRunSync()

      ex.shouldBe(CircuitBreaker.RejectedException)
      ex.getMessage.shouldBe("CircuitBreaker rejected")
    }
  }
}
