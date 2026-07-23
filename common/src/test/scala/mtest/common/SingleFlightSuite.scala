package mtest.common

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.IORuntime
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.resilience.SingleFlight
import munit.CatsEffectSuite

import scala.concurrent.duration.DurationInt

class SingleFlightSuite extends CatsEffectSuite {

  implicit val runtime: IORuntime = IORuntime.global

  test("1.SingleFlight deduplicates concurrent calls") {
    val prom = for {
      sf <- SingleFlight[IO, Int]
      counter <- Ref.of[IO, Int](0)
      effect = counter.updateAndGet(_ + 1) // side-effecting effect
      // Run 5 concurrent fibers
      results <- List.fill(5)(sf(effect)).parSequence
      finalCount <- counter.get
      _ <- sf.isBusy
    } yield {
      // All fibers should get the same value
      assert(results.forall(_ == 1), s"results = ${results.mkString(",")}")

      // The effect ran only once
      assertEquals(finalCount, 1)
    }
    prom.unsafeRunSync()
  }

  test("2.SingleFlight propagates errors to all followers") {
    val prom = for {
      sf <- SingleFlight.apply[IO, Int]

      failing = IO.raiseError[Int](new RuntimeException("boom"))

      results <- List.fill(3)(sf(failing).attempt).parSequence
    } yield results.foreach {
      case Left(e)  => assertEquals(e.getMessage, "boom")
      case Right(_) => fail("Should not succeed")
    }
    prom.unsafeRunSync()
  }

  test("3.SingleFlight allows new calls after completion") {
    val prom = for {
      sf <- SingleFlight.apply[IO, Int]
      counter <- Ref.of[IO, Int](0)
      effect = counter.updateAndGet(_ + 1)
      _ <- sf(effect) // first call runs effect
      _ <- sf(effect) // second call runs effect again
      finalCount <- counter.get
    } yield
      // Each call after completion should be able to run a new effect
      assertEquals(finalCount, 2)

    prom.unsafeRunSync()
  }

  test("4.SingleFlight should unblock followers when leader is canceled") {
    val prom = for {
      sf <- SingleFlight.apply[IO, Int]
      leader <- sf(IO.never[Int]).attempt.start
      _ <- IO.sleep(50.millis)
      follower <- sf(IO.pure(42)).attempt.start
      _ <- leader.cancel
      outcome <- follower.joinWithNever.timeout(1.second)
    } yield {
      assert(outcome.isLeft)
      assertEquals(outcome.swap.toOption.get.getMessage, "SingleFlight leader fiber canceled")
    }

    prom.unsafeRunSync()
  }

  test("5.SingleFlight tryApply should return None when busy") {
    val prom = for {
      sf <- SingleFlight.apply[IO, Int]
      running <- sf(IO.sleep(300.millis) >> IO.pure(1)).start
      _ <- IO.sleep(50.millis)
      immediate <- sf.tryApply(IO.pure(2))
      _ <- running.joinWithNever
    } yield assertEquals(immediate, None)

    prom.unsafeRunSync()
  }

  test("6.SingleFlight tryApply should run effect when idle") {
    val prom = for {
      sf <- SingleFlight.apply[IO, Int]
      counter <- Ref.of[IO, Int](0)
      result <- sf.tryApply(counter.updateAndGet(_ + 1))
      finalCount <- counter.get
    } yield {
      assertEquals(result, Some(1))
      assertEquals(finalCount, 1)
    }

    prom.unsafeRunSync()
  }

  test("7.SingleFlight tryApply should propagate leader errors") {
    val prom = for {
      sf <- SingleFlight.apply[IO, Int]
      result <- sf.tryApply(IO.raiseError[Int](new RuntimeException("boom"))).attempt
    } yield {
      assert(result.isLeft)
      assertEquals(result.swap.toOption.get.getMessage, "boom")
    }

    prom.unsafeRunSync()
  }

  test("8.SingleFlight isBusy should reflect in-flight lifecycle") {
    val prom = for {
      sf <- SingleFlight.apply[IO, Int]
      before <- sf.isBusy
      running <- sf(IO.sleep(200.millis) >> IO.pure(1)).start
      _ <- IO.sleep(50.millis)
      during <- sf.isBusy
      _ <- running.joinWithNever
      after <- sf.isBusy
    } yield {
      assertEquals(before, false)
      assertEquals(during, true)
      assertEquals(after, false)
    }

    prom.unsafeRunSync()
  }

  test("9.SingleFlight high contention should execute once per wave") {
    val prom = for {
      sf <- SingleFlight.apply[IO, Int]
      counter <- Ref.of[IO, Int](0)
      effect = IO.sleep(20.millis) >> counter.updateAndGet(_ + 1)
      wave1 <- List.fill(200)(sf(effect)).parSequence
      wave2 <- List.fill(200)(sf(effect)).parSequence
      finalCount <- counter.get
    } yield {
      assertEquals(wave1.distinct, List(1))
      assertEquals(wave2.distinct, List(2))
      assertEquals(finalCount, 2)
    }

    prom.unsafeRunSync()
  }
}
