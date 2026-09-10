package mtest.guard

import cats.Applicative
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits.{catsSyntaxApplicativeId, toTraverseOps}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{BatchKind, BatchMode, PostConditionUnsatisfied}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

class BatchLightMonadicTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("batch-light")

  "monadic" - {
    "smoke" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light")
          .monadic { job =>
            for {
              _ <- job.pure(1)
              a <- job("a", IO(1))
              _ <- 2.pure[job.Monadic]
              b <- job("b", IO(2))
              _ <- job.pure(3)
              c <- job("c", IO(3))
              _ <- List(1, 2, 3).traverse(job.pure)
            } yield a + b + c
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.result shouldBe Right(6)
            monadicValue.jobs.size shouldBe 3
            monadicValue.jobs.map(_.job.name) shouldBe List("a", "b", "c")
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "completed jobs are ordered by index" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-ordered")
          .monadic { job =>
            for {
              a <- job("a", IO(1))
              b <- job("b", IO(2))
              c <- job("c", IO(3))
            } yield a + b + c
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.jobs.map(_.job.index) shouldBe List(1, 2, 3)
            monadicValue.jobs.map(_.job.name) shouldBe List("a", "b", "c")
            monadicValue.jobs.map(_.succeeded) shouldBe List(true, true, true)
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "timing is preserved in the batch report" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-timing")
          .monadic { job =>
            for {
              a <- job("a", IO.sleep(50.millis).as(1))
              b <- job("b", IO.sleep(50.millis).as(2))
            } yield a + b
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.spent.toMillis should be > 0L
            monadicValue.jobs.map(_.took.toMillis).forall(_ > 0L) shouldBe true
            monadicValue.jobs.map(_.job.name) shouldBe List("a", "b")
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "spent counts invisible lift steps between jobs" in {
      // Regression: the old spent summed per-job took, which dropped the wall-clock
      // consumed by invisible lift/pure steps. spent is now the full span, so a 200ms
      // lifted sleep sandwiched between two fast jobs must show up in spent.
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-invisible-lift")
          .monadic { job =>
            for {
              a <- job("a", IO(1))
              _ <- job.untracked(IO.sleep(200.millis))
              b <- job("b", IO(2))
            } yield a + b
          }
          .monadicBatch
          .map { mb =>
            // only the two visible jobs are recorded
            mb.jobs.map(_.job.name) shouldBe List("a", "b")
            // the invisible 200ms sleep is captured in the span
            mb.spent.toMillis should be >= 200L
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "sum of per-job took equals spent (gaps redistributed)" in {
      // monadicHistory rewrites each job's start to the previous job's end, so the
      // per-job took values are contiguous and telescope exactly to spent.
      //
      // Alignment guard: BatchLight and Batch share the same timing model. This exact-nanos
      // equality must hold identically here and in BatchTest "25.monadic sum of per-job took
      // equals spent". If one changes, both must — do not let the two variants drift apart.
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-took-sum")
          .monadic { job =>
            for {
              a <- job("a", IO.sleep(30.millis).as(1))
              _ <- job.pure(())
              _ <- job.untracked(IO.sleep(80.millis))
              b <- job("b", IO.sleep(30.millis).as(2))
              c <- job("c", IO.sleep(30.millis).as(3))
            } yield a + b + c
          }
          .monadicBatch
          .map { mb =>
            val sumTook = mb.jobs.map(_.took.toNanos).sum
            sumTook shouldBe mb.spent.toNanos
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "a later job's took absorbs the preceding invisible gap" in {
      // The gap left by an invisible step lands in the following visible job's took.
      // Here b runs ~20ms but is preceded by a 150ms invisible sleep, so b's took
      // must reflect the gap, not just b's own execution time.
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-gap-absorb")
          .monadic { job =>
            for {
              a <- job("a", IO.sleep(20.millis).as(1))
              _ <- job.untracked(IO.sleep(150.millis))
              b <- job("b", IO.sleep(20.millis).as(2))
            } yield a + b
          }
          .monadicBatch
          .map { mb =>
            val tookByName = mb.jobs.map(js => js.job.name -> js.took.toMillis).toMap
            // b absorbs the 150ms gap plus its own ~20ms
            tookByName("b") should be >= 150L
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "single-job monadic batch spent matches that job's took" in {
      // Edge of monadicHistory: with one visible job there is nothing to redistribute,
      // so spent equals the single job's took exactly.
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-single-job")
          .monadic { job =>
            job("only", IO.sleep(40.millis).as(1))
          }
          .monadicBatch
          .map { mb =>
            mb.jobs.size shouldBe 1
            mb.jobs.head.took.toNanos shouldBe mb.spent.toNanos
            mb.spent.toMillis should be >= 40L
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "exception" in {
      var aExecuted = false
      var bExecuted = false
      var cExecuted = false

      val se = service.eventStream { agent =>
        agent
          .batchLight("light-exception")
          .monadic { job =>
            for {
              a <- job("a", IO { aExecuted = true; 1 })
              b <- job("b", IO { bExecuted = true } *> IO.raiseError[Int](new Exception("boom")))
              c <- job("c", IO { cExecuted = true; 3 })
            } yield a + b + c
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.result.isLeft shouldBe true
            aExecuted shouldBe true
            bExecuted shouldBe true
            cExecuted shouldBe false
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "a rejected predicate marks the job failed but does not stop the chain" in {
      var aExecuted = false
      var bExecuted = false
      var cExecuted = false

      val se = service.eventStream { agent =>
        agent
          .batchLight("light-mix")
          .monadic { job =>
            for {
              a <- job("a", IO { aExecuted = true; 1 })
              b <- job("b", IO { bExecuted = true; 2 }, _ => false)
              c <- job("c", IO { cExecuted = true; 3 })
            } yield a + b + c
          }
          .monadicBatch
          .map { monadicValue =>
            // the rejected value still flows through, so the chain completes
            monadicValue.result shouldBe Right(6)
            monadicValue.jobs.size shouldBe 3
            monadicValue.jobs.head.succeeded.shouldBe(true)
            monadicValue.jobs(1).succeeded.shouldBe(false)
            monadicValue.jobs(2).succeeded.shouldBe(true)
            aExecuted shouldBe true
            bExecuted shouldBe true
            cExecuted shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "supports applicative-style composition" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-applicative")
          .monadic { job =>
            type M[A] = job.Monadic[A]
            val combined = Applicative[M].map2(job("a", IO(1)), job("b", IO(2)))(_ + _)
            combined
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.result shouldBe Right(3)
            monadicValue.jobs.size shouldBe 2
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "identity law preserves the job result" in {
      val se = service.eventStream { agent =>
        val left = agent
          .batchLight("light-applicative-identity")
          .monadic { job =>
            Applicative[job.Monadic].ap(Applicative[job.Monadic].pure((x: Int) => x))(job("a", IO(1)))
          }
          .monadicBatch
          .map(_.result)

        val right = agent
          .batchLight("light-applicative-identity-right")
          .monadic { job =>
            job("a", IO(1))
          }
          .monadicBatch
          .map(_.result)

        val combined = for {
          l <- left
          r <- right
        } yield {
          l shouldBe Right(1)
          r shouldBe Right(1)
          ()
        }

        combined
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "withFilter on a pure value should fail without crashing (light-1)" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-filter-pure")
          .monadic { job =>
            job.pure(1).withFilter(_ => false)
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.result.isLeft shouldBe true
            monadicValue.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied] shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "withFilter should fail when predicate is not satisfied" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-filter")
          .monadic { job =>
            for {
              a <- job("a", IO(1))
              b <- job("b", IO(false))
              if b
              c <- job("c", IO(3))
            } yield a + c
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.result.isLeft shouldBe true
            monadicValue.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied] shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "withFilter on a pure value should fail without crashing (light-2)" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-filter-pure")
          .monadic { job =>
            job.pure(1).withFilter(_ => false)
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.result.isLeft shouldBe true
            monadicValue.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied] shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "a satisfied predicate marks the job succeeded" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-tuple")
          .monadic { job =>
            for {
              a <- job("a", IO(1))
              b <- job("b", IO(2), _ > 0)
              c <- job("c", IO(3))
            } yield a + b + c
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.result shouldBe Right(6)
            monadicValue.jobs.size shouldBe 3
            // all monadic jobs are Value
            monadicValue.jobs.map(_.job.kind) shouldBe List.fill(3)(BatchKind.Value)
            monadicValue.jobs.map(_.job.mode) shouldBe List.fill(3)(BatchMode.Monadic)
            monadicValue.jobs(1).succeeded.shouldBe(true)
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }
  }

  "sequential" - {
    "quasiBatch should support rename and predicate" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-sequential")
          .sequential("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
          .withPostCondition(_ >= 2)
          .quasiBatch
          .map { state =>
            state.jobs.size shouldBe 3
            state.jobs.head.record.job.name shouldBe "a"
            state.jobs.head.record.job.mode shouldBe BatchMode.Sequential
            state.jobs.head.record.job.kind shouldBe BatchKind.Quasi
            state.jobs.head.record.succeeded shouldBe false
            state.jobs(1).record.succeeded shouldBe true
            state.jobs(2).record.succeeded shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "completed jobs are ordered by index" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-sequential-ordered")
          .sequential("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
          .quasiBatch
          .map { state =>
            state.jobs.map(_.record.job.index) shouldBe List(1, 2, 3)
            state.jobs.map(_.record.job.name) shouldBe List("a", "b", "c")
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "valueBatch should fail when predicate is not satisfied" in {
      val se = service.eventStreamR { agent =>
        Resource.eval(
          agent
            .batchLight("light-sequential-value")
            .sequential("a" -> IO(1), "b" -> IO(2))
            .withPostCondition(_ > 1)
            .valueBatch
            .attempt
            .map { outcome =>
              outcome.fold(_.isInstanceOf[PostConditionUnsatisfied], _ => false) shouldBe true
            }
        )
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "valueBatch should return all values on success" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-sequential-value-ok")
          .sequential("a" -> IO(10), "b" -> IO(20), "c" -> IO(30))
          .valueBatch
          .map { bv =>
            bv.jobs.size shouldBe 3
            bv.jobs.map(_.result) shouldBe List(10, 20, 30)
            bv.mode shouldBe BatchMode.Sequential
            bv.jobs.map(_.record.job.kind) shouldBe List.fill(3)(BatchKind.Value)
            bv.jobs.map(_.record.job.mode) shouldBe List.fill(3)(BatchMode.Sequential)
            bv.succeeded shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }
  }

  "parallel" - {
    "parallel(0) should fail fast" in {
      val se = service.eventStreamR { agent =>
        Resource.eval(
          agent
            .batchLight("light-parallel-invalid")
            .parallel(0)("a" -> IO(1))
            .quasiBatch
            .attempt
            .map { outcome =>
              outcome.fold(_.getMessage.contains("parallelism must be > 0"), _ => false) shouldBe true
            }
        )
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 3
    }

    "parallel(fas*) should fail fast when empty" in {
      val se = service.eventStreamR { agent =>
        Resource.eval(
          agent
            .batchLight("light-parallel-empty")
            .parallel[Int]()
            .quasiBatch
            .attempt
            .map { outcome =>
              outcome.fold(_.getMessage.contains("parallelism must be > 0"), _ => false) shouldBe true
            }
        )
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 3
    }

    "parallel(fas*) should create parallel mode using input size" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-parallel-default")
          .parallel("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
          .withPostCondition(_ >= 2)
          .quasiBatch
          .map { state =>
            state.jobs.size shouldBe 3
            state.jobs.head.record.job.name shouldBe "a"
            state.jobs.head.record.job.mode shouldBe BatchMode.Parallel(3)
            state.jobs.head.record.job.kind shouldBe BatchKind.Quasi
            state.jobs.head.record.succeeded shouldBe false
            state.jobs(1).record.succeeded shouldBe true
            state.jobs(2).record.succeeded shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "parallel(parallelism)(fas*) should use explicit parallelism" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-parallel-explicit")
          .parallel(1)("a" -> IO(1), "b" -> IO(2))
          .valueBatch
          .map { value =>
            value.jobs.size shouldBe 2
            value.mode shouldBe BatchMode.Parallel(1)
            value.jobs.map(_.record.job.kind) shouldBe List.fill(2)(BatchKind.Value)
            value.jobs.map(_.record.job.mode) shouldBe List.fill(2)(BatchMode.Parallel(1))
            value.jobs.map(_.result).sum shouldBe 3
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "completed jobs are ordered by index" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-parallel-ordered")
          .parallel(3)("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
          .quasiBatch
          .map { state =>
            state.jobs.map(_.record.job.index) shouldBe List(1, 2, 3)
            state.jobs.map(_.record.job.name) shouldBe List("a", "b", "c")
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "valueBatch should fail when predicate is not satisfied" in {
      val se = service.eventStreamR { agent =>
        Resource.eval(
          agent
            .batchLight("light-parallel-value")
            .parallel(2)("a" -> IO(1), "b" -> IO(2))
            .withPostCondition(_ > 1)
            .valueBatch
            .attempt
            .map { outcome =>
              outcome.fold(_.isInstanceOf[PostConditionUnsatisfied], _ => false) shouldBe true
            }
        )
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "failed action cancels sibling jobs" in {
      var aCompleted = false
      var cCompleted = false

      val se = service.eventStreamR { agent =>
        Resource.eval(
          agent
            .batchLight("light-failed-cancels-siblings")
            .parallel(3)(
              "a" -> IO.sleep(2.seconds) *> IO { aCompleted = true; 1 },
              "b" -> IO.raiseError[Int](new Exception("boom")),
              "c" -> IO.sleep(5.seconds) *> IO { cCompleted = true; 3 }
            )
            .valueBatch
            .attempt
            .map { outcome =>
              outcome.isLeft shouldBe true
              aCompleted shouldBe false
              cCompleted shouldBe false
            }
        )
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }
  }
}
