package mtest.common

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.IORuntime
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.SingleFlight
import munit.CatsEffectSuite

class SingleFlightSuite extends CatsEffectSuite {

  implicit val runtime: IORuntime = IORuntime.global

  test("SingleFlight deduplicates concurrent calls") {
    val prom = for {
      sf <- SingleFlight.create[IO, Int]
      counter <- Ref.of[IO, Int](0)
      effect = counter.updateAndGet(_ + 1) // side-effecting effect
      // Run 5 concurrent fibers
      results <- List.fill(5)(sf(effect)).parSequence
      finalCount <- counter.get
    } yield {
      // All fibers should get the same value
      assert(results.forall(_ == 1), s"results = ${results.mkString(",")}")

      // The effect ran only once
      assertEquals(finalCount, 1)
    }
    prom.unsafeRunSync()
  }

  test("SingleFlight propagates errors to all followers") {
    val prom = for {
      sf <- SingleFlight.create[IO, Int]

      failing = IO.raiseError[Int](new RuntimeException("boom"))

      results <- List.fill(3)(sf(failing).attempt).parSequence
    } yield results.foreach {
      case Left(e)  => assertEquals(e.getMessage, "boom")
      case Right(_) => fail("Should not succeed")
    }
    prom.unsafeRunSync()
  }

  test("SingleFlight allows new calls after completion") {
    val prom = for {
      sf <- SingleFlight.create[IO, Int]
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
}
