package mtest.common

import cats.effect.IO
import cats.effect.kernel.{Deferred, Ref}
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

  test("4.canceling the first caller does not cancel shared work for followers") {
    val prom = cats.effect.testkit.TestControl.executeEmbed {
      for {
        sf <- SingleFlight.apply[IO, Int]
        started <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        worker_canceled <- Deferred[IO, Unit]
        shared = (started.complete(()).void *> release.get.as(42))
          .onCancel(worker_canceled.complete(()).void)
        first <- sf(shared).start
        _ <- started.get
        cancel_first <- (IO.sleep(1.second) *> first.cancel).start
        follower <- sf(IO.pure(99)).start
        _ <- cancel_first.joinWithNever
        canceled_before_release <- worker_canceled.tryGet
        _ <- release.complete(())
        result <- follower.joinWithNever.timeout(1.second)
        busy <- sf.isBusy
      } yield {
        assertEquals(canceled_before_release, None)
        assertEquals(result, 42)
        assertEquals(busy, false)
      }
    }

    prom.unsafeRunSync()
  }

  test("4a.canceling the sole waiter waits for cleanup before starting a new flight") {
    val prom = cats.effect.testkit.TestControl.executeEmbed {
      for {
        sf <- SingleFlight.apply[IO, Int]
        started <- Deferred[IO, Unit]
        cleanup_started <- Deferred[IO, Unit]
        allow_cleanup <- Deferred[IO, Unit]
        cancellation_finished <- Deferred[IO, Unit]
        replacement_started <- Deferred[IO, Unit]
        shared = (started.complete(()).void *> IO.never[Int]).onCancel(
          cleanup_started.complete(()).void *> allow_cleanup.get)
        only_waiter <- sf(shared).start
        _ <- started.get
        cancellation <- (only_waiter.cancel *> cancellation_finished.complete(()).void).start
        _ <- cleanup_started.get.timeout(1.second)
        cancellation_before_cleanup <- cancellation_finished.tryGet
        replacement <- sf(replacement_started.complete(()).void.as(42)).start
        _ <- IO.sleep(1.second)
        replacement_before_cleanup <- replacement_started.tryGet
        _ <- allow_cleanup.complete(())
        _ <- cancellation.joinWithNever.timeout(1.second)
        result <- replacement.joinWithNever.timeout(1.second)
        replacement_after_cleanup <- replacement_started.tryGet
        busy <- sf.isBusy
      } yield {
        assertEquals(cancellation_before_cleanup, None)
        assertEquals(replacement_before_cleanup, None)
        assertEquals(replacement_after_cleanup, Some(()))
        assertEquals(result, 42)
        assertEquals(busy, false)
      }
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
