package com.github.chenharryhua.nanjin.common.resilience

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class CircuitBreakerInternalSpec extends AnyFreeSpec with Matchers {

  "CircuitBreaker rejection classification" - {

    "maps non-broken states to RejectedException" in {
      val state = CircuitBreaker.State.Open
      val ex = CircuitBreaker.rejectionForState(state)

      ex shouldBe a[CircuitBreaker.RejectedException]
      ex.asInstanceOf[CircuitBreaker.RejectedException].state shouldBe state
    }

    "maps Broken state to broken exception" in {
      val ex = CircuitBreaker.rejectionForState(CircuitBreaker.State.Broken)

      ex.getMessage shouldBe "CircuitBreaker is broken"
    }
  }
}
