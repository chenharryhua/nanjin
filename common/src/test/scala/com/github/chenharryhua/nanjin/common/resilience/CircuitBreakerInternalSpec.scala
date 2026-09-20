package com.github.chenharryhua.nanjin.common.resilience

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.Policy
import munit.CatsEffectSuite

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

class CircuitBreakerInternalSpec extends CatsEffectSuite {

  test("CircuitBreaker rejection classification: maps rejection to singleton RejectedException") {
    CircuitBreaker[IO](
      ZoneId.systemDefault(),
      maxFailures = 1,
      _ => Policy.fixedDelay(10.seconds)
    ).use { cb =>
      for {
        _ <- cb.attempt(IO.raiseError(new RuntimeException("fail")))
        _ <- cb.attempt(IO.raiseError(new RuntimeException("fail")))
        rejected <- cb.attempt(IO.unit)
      } yield rejected.swap.toOption.get
    }.map { ex =>
      assertEquals(ex, CircuitBreaker.RejectedException)
      assertEquals(ex.getMessage, "CircuitBreaker rejected")
    }
  }
}
