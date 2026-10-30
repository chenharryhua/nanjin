package com.github.chenharryhua.nanjin.common.resilience

import cats.effect.IO
import cats.effect.kernel.{Deferred, Ref}
import munit.CatsEffectSuite

class ResilienceNoopTest extends CatsEffectSuite {

  test("1.Retry.noop preserves success and failure and evaluates each effect once") {
    val failure = new RuntimeException("retry-noop")

    for {
      evaluations <- Ref.of[IO, Int](0)
      success <- Retry.noop[IO](evaluations.updateAndGet(_ + 1))
      failed <- Retry.noop[IO](evaluations.update(_ + 1) >> IO.raiseError[Int](failure)).attempt
      count <- evaluations.get
    } yield {
      assertEquals(success, 1)
      assertEquals(failed, Left(failure))
      assertEquals(count, 2)
    }
  }

  test("2.Retry.noop propagates cancellation to the supplied effect") {
    for {
      started <- Deferred[IO, Unit]
      canceled <- Deferred[IO, Unit]
      fiber <- Retry.noop[IO](
        (started.complete(()).void >> IO.never[Int])
          .onCancel(canceled.complete(()).void)).start
      _ <- started.get
      _ <- fiber.cancel
      result <- canceled.tryGet
    } yield assertEquals(result, Some(()))
  }

  test("3.CircuitBreaker.noop preserves protect/attempt behavior and reports Closed(0)") {
    val failure = new RuntimeException("circuit-noop")
    val breaker = CircuitBreaker.noop[IO]

    for {
      protected_value <- breaker.protect(IO.pure(42))
      attempted_success <- breaker.attempt(IO.pure(7))
      attempted_failure <- breaker.attempt(IO.raiseError[Int](failure))
      state <- breaker.state
    } yield {
      assertEquals(protected_value, 42)
      assertEquals(attempted_success, Right(7))
      assertEquals(attempted_failure, Left(failure))
      assertEquals(state, CircuitBreaker.State.Closed(0))
    }
  }

  test("4.CircuitBreaker.noop propagates cancellation") {
    val breaker = CircuitBreaker.noop[IO]

    for {
      started <- Deferred[IO, Unit]
      canceled <- Deferred[IO, Unit]
      fiber <- breaker.protect(
        (started.complete(()).void >> IO.never[Int])
          .onCancel(canceled.complete(()).void)).start
      _ <- started.get
      _ <- fiber.cancel
      result <- canceled.tryGet
    } yield assertEquals(result, Some(()))
  }

  test("5.SingleFlight.noop runs every apply and tryApply argument independently") {
    val single_flight = SingleFlight.noop[IO, Int]
    val failure = new RuntimeException("single-flight-noop")

    for {
      evaluations <- Ref.of[IO, Int](0)
      first <- single_flight(evaluations.updateAndGet(_ + 1))
      second <- single_flight(evaluations.updateAndGet(_ + 1))
      tried <- single_flight.tryApply(evaluations.updateAndGet(_ + 1))
      apply_failure <- single_flight(IO.raiseError[Int](failure)).attempt
      try_failure <- single_flight.tryApply(IO.raiseError[Int](failure)).attempt
      count <- evaluations.get
    } yield {
      assertEquals(List(first, second), List(1, 2))
      assertEquals(tried, Some(3))
      assertEquals(apply_failure, Left(failure))
      assertEquals(try_failure, Left(failure))
      assertEquals(count, 3)
    }
  }
}
