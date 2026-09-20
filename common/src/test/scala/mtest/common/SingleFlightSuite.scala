package mtest.common

import cats.effect.IO
import cats.effect.kernel.{Deferred, Ref}
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.resilience.SingleFlight
import munit.CatsEffectSuite

import scala.concurrent.duration.DurationInt

final class SingleFlightSuite extends CatsEffectSuite {

  test("1.SingleFlight deduplicates concurrent calls") {
    TestControl.executeEmbed {
      for {
        single_flight <- SingleFlight[IO, Int]
        counter <- Ref.of[IO, Int](0)
        effect = IO.sleep(1.second) *> counter.updateAndGet(_ + 1)
        results <- List.fill(5)(single_flight(effect)).parSequence
        final_count <- counter.get
      } yield {
        assertEquals(results, List.fill(5)(1))
        assertEquals(final_count, 1)
      }
    }
  }

  test("2.SingleFlight propagates errors to all followers") {
    TestControl.executeEmbed {
      for {
        single_flight <- SingleFlight[IO, Int]
        failing = IO.sleep(1.second) *> IO.raiseError[Int](new RuntimeException("boom"))
        results <- List.fill(3)(single_flight(failing).attempt).parSequence
      } yield results.foreach {
        case Left(error) => assertEquals(error.getMessage, "boom")
        case Right(_)    => fail("should not succeed")
      }
    }
  }

  test("3.SingleFlight allows new calls after completion") {
    for {
      single_flight <- SingleFlight[IO, Int]
      counter <- Ref.of[IO, Int](0)
      effect = counter.updateAndGet(_ + 1)
      _ <- single_flight(effect)
      _ <- single_flight(effect)
      final_count <- counter.get
    } yield assertEquals(final_count, 2)
  }

  test("4.canceling the first caller does not cancel shared work for followers") {
    TestControl.executeEmbed {
      for {
        single_flight <- SingleFlight[IO, Int]
        started <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        worker_canceled <- Deferred[IO, Unit]
        shared = (started.complete(()).void *> release.get.as(42))
          .onCancel(worker_canceled.complete(()).void)
        first <- single_flight(shared).start
        _ <- started.get
        cancel_first <- (IO.sleep(1.second) *> first.cancel).start
        follower <- single_flight(IO.pure(99)).start
        _ <- cancel_first.joinWithNever
        canceled_before_release <- worker_canceled.tryGet
        _ <- release.complete(())
        result <- follower.joinWithNever.timeout(1.second)
      } yield {
        assertEquals(canceled_before_release, None)
        assertEquals(result, 42)
      }
    }
  }

  test("4a.canceling the sole waiter waits for cleanup before starting a new flight") {
    TestControl.executeEmbed {
      for {
        single_flight <- SingleFlight[IO, Int]
        started <- Deferred[IO, Unit]
        cleanup_started <- Deferred[IO, Unit]
        allow_cleanup <- Deferred[IO, Unit]
        cancellation_finished <- Deferred[IO, Unit]
        replacement_started <- Deferred[IO, Unit]
        shared = (started.complete(()).void *> IO.never[Int]).onCancel(
          cleanup_started.complete(()).void *> allow_cleanup.get)
        only_waiter <- single_flight(shared).start
        _ <- started.get
        cancellation <- (only_waiter.cancel *> cancellation_finished.complete(()).void).start
        _ <- cleanup_started.get.timeout(1.second)
        cancellation_before_cleanup <- cancellation_finished.tryGet
        replacement <- single_flight(replacement_started.complete(()).void.as(42)).start
        _ <- IO.sleep(1.second)
        replacement_before_cleanup <- replacement_started.tryGet
        _ <- allow_cleanup.complete(())
        _ <- cancellation.joinWithNever.timeout(1.second)
        result <- replacement.joinWithNever.timeout(1.second)
        replacement_after_cleanup <- replacement_started.tryGet
      } yield {
        assertEquals(cancellation_before_cleanup, None)
        assertEquals(replacement_before_cleanup, None)
        assertEquals(replacement_after_cleanup, Some(()))
        assertEquals(result, 42)
      }
    }
  }

  test("5.tryApply returns None without evaluating its argument when busy") {
    TestControl.executeEmbed {
      for {
        single_flight <- SingleFlight[IO, Int]
        started <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        rejected_evaluated <- Ref.of[IO, Boolean](false)
        running <- single_flight(started.complete(()).void *> release.get.as(1)).start
        _ <- started.get
        immediate <- single_flight.tryApply(rejected_evaluated.set(true).as(2))
        _ <- release.complete(())
        result <- running.joinWithNever
        _ <- IO.sleep(1.second)
        evaluated <- rejected_evaluated.get
      } yield {
        assertEquals(immediate, None)
        assertEquals(evaluated, false)
        assertEquals(result, 1)
      }
    }
  }

  test("6.tryApply runs its argument when idle") {
    for {
      single_flight <- SingleFlight[IO, Int]
      counter <- Ref.of[IO, Int](0)
      result <- single_flight.tryApply(counter.updateAndGet(_ + 1))
      final_count <- counter.get
    } yield {
      assertEquals(result, Some(1))
      assertEquals(final_count, 1)
    }
  }

  test("7.tryApply propagates worker errors") {
    for {
      single_flight <- SingleFlight[IO, Int]
      result <- single_flight.tryApply(IO.raiseError[Int](new RuntimeException("boom"))).attempt
    } yield {
      assert(result.isLeft)
      assertEquals(result.swap.toOption.get.getMessage, "boom")
    }
  }

  test("8.SingleFlight executes once per high-contention wave") {
    TestControl.executeEmbed {
      for {
        single_flight <- SingleFlight[IO, Int]
        counter <- Ref.of[IO, Int](0)
        effect = IO.sleep(1.second) *> counter.updateAndGet(_ + 1)
        wave_1 <- List.fill(200)(single_flight(effect)).parSequence
        wave_2 <- List.fill(200)(single_flight(effect)).parSequence
        final_count <- counter.get
      } yield {
        assertEquals(wave_1.distinct, List(1))
        assertEquals(wave_2.distinct, List(2))
        assertEquals(final_count, 2)
      }
    }
  }
}
