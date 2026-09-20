package mtest.guard

import cats.Applicative
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.implicits.{catsSyntaxApplicativeId, toTraverseOps}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{BatchKind, BatchMode, PostConditionUnsatisfied}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import munit.CatsEffectSuite

import scala.concurrent.duration.DurationInt

class BatchLightMonadicTest extends CatsEffectSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("batch-light")

  test("monadic: smoke") {
    service.eventStream { agent =>
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
          assertEquals(monadicValue.result, Right(6))
          assertEquals(monadicValue.outcomes.size, 3)
          assertEquals(monadicValue.outcomes.map(_.record.job.name), List("a", "b", "c"))
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic: completed jobs are ordered by index") {
    service.eventStream { agent =>
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
          assertEquals(monadicValue.outcomes.map(_.record.job.index), List(1, 2, 3))
          assertEquals(monadicValue.outcomes.map(_.record.job.name), List("a", "b", "c"))
          assertEquals(monadicValue.outcomes.map(_.record.succeeded), List(true, true, true))
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic: timing is preserved in the batch report") {
    service.eventStream { agent =>
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
          assert(monadicValue.spent.toMillis > 0L)
          assert(monadicValue.outcomes.map(_.record.took.toMillis).forall(_ > 0L))
          assertEquals(monadicValue.outcomes.map(_.record.job.name), List("a", "b"))
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic: spent counts invisible lift steps between jobs") {
    // Regression: the old spent summed per-job took, which dropped the wall-clock
    // consumed by invisible lift/pure steps. spent is now the full span, so a 200ms
    // lifted sleep sandwiched between two fast jobs must show up in spent.
    service.eventStream { agent =>
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
          assertEquals(mb.outcomes.map(_.record.job.name), List("a", "b"))
          // the invisible 200ms sleep is captured in the span
          assert(mb.spent.toMillis >= 200L)
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic: sum of per-job took telescopes to the span through the last job, within spent") {
    // monadicHistory rewrites each job's start to the previous job's end, so the per-job
    // took values are contiguous and telescope to the span from the first job's start to
    // the last job's end. spent is measured against a fresh clock reading taken after the
    // whole chain finishes, so it also covers trailing framing that follows the last job;
    // sumTook is therefore <= spent, with only a tiny remainder.
    //
    // Alignment guard: BatchLight and Batch share the same timing model. This relationship
    // must hold identically here and in BatchTest "25.monadic sum of per-job took within
    // spent". If one changes, both must — do not let the two variants drift apart.
    service.eventStream { agent =>
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
          val sumTook = mb.outcomes.map(_.record.took.toNanos).sum
          assert(sumTook <= mb.spent.toNanos)
          // the trailing remainder is bookkeeping only, far below the ~170ms of real work
          assert((mb.spent.toNanos - sumTook) < 50_000_000L) // 50ms
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic: a later job's took absorbs the preceding invisible gap") {
    // The gap left by an invisible step lands in the following visible job's took.
    // Here b runs ~20ms but is preceded by a 150ms invisible sleep, so b's took
    // must reflect the gap, not just b's own execution time.
    service.eventStream { agent =>
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
          val tookByName = mb.outcomes.map(js => js.record.job.name -> js.record.took.toMillis).toMap
          // b absorbs the 150ms gap plus its own ~20ms
          assert(tookByName("b") >= 150L)
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic: single-job monadic batch spent covers that job's took, within a tiny remainder") {
    // Edge of monadicHistory: with one visible job there is nothing to redistribute, so the
    // single job's took is the whole through-last-job span. spent adds only the trailing
    // framing captured by the fresh post-chain reading, so took <= spent by a tiny margin.
    service.eventStream { agent =>
      agent
        .batchLight("light-single-job")
        .monadic { job =>
          job("only", IO.sleep(40.millis).as(1))
        }
        .monadicBatch
        .map { mb =>
          assertEquals(mb.outcomes.size, 1)
          assert(mb.outcomes.head.record.took.toNanos <= mb.spent.toNanos)
          assert((mb.spent.toNanos - mb.outcomes.head.record.took.toNanos) < 50_000_000L) // 50ms
          assert(mb.spent.toMillis >= 40L)
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic: exception") {
    var aExecuted = false
    var bExecuted = false
    var cExecuted = false

    service.eventStream { agent =>
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
          assert(monadicValue.result.isLeft)
          assert(aExecuted)
          assert(bExecuted)
          assert(!cExecuted)
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic: a rejected predicate marks the job failed but does not stop the chain") {
    var aExecuted = false
    var bExecuted = false
    var cExecuted = false

    service.eventStream { agent =>
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
          assertEquals(monadicValue.result, Right(6))
          assertEquals(monadicValue.outcomes.size, 3)
          assert(monadicValue.outcomes.head.record.succeeded)
          assert(!monadicValue.outcomes(1).record.succeeded)
          assert(monadicValue.outcomes(2).record.succeeded)
          assert(aExecuted)
          assert(bExecuted)
          assert(cExecuted)
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic: supports applicative-style composition") {
    service.eventStream { agent =>
      agent
        .batchLight("light-applicative")
        .monadic { job =>
          type M[A] = job.Monadic[A]
          val combined = Applicative[M].map2(job("a", IO(1)), job("b", IO(2)))(_ + _)
          combined
        }
        .monadicBatch
        .map { monadicValue =>
          assertEquals(monadicValue.result, Right(3))
          assertEquals(monadicValue.outcomes.size, 2)
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic: identity law preserves the job result") {
    service.eventStream { agent =>
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

      for {
        l <- left
        r <- right
      } yield {
        assertEquals(l, Right(1))
        assertEquals(r, Right(1))
        ()
      }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic: withFilter on a pure value should fail without crashing (light-1)") {
    service.eventStream { agent =>
      agent
        .batchLight("light-filter-pure")
        .monadic { job =>
          job.pure(1).withFilter(_ => false)
        }
        .monadicBatch
        .map { monadicValue =>
          assert(monadicValue.result.isLeft)
          assert(monadicValue.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied])
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic: withFilter should fail when predicate is not satisfied") {
    service.eventStream { agent =>
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
          assert(monadicValue.result.isLeft)
          assert(monadicValue.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied])
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic: withFilter on a pure value should fail without crashing (light-2)") {
    service.eventStream { agent =>
      agent
        .batchLight("light-filter-pure")
        .monadic { job =>
          job.pure(1).withFilter(_ => false)
        }
        .monadicBatch
        .map { monadicValue =>
          assert(monadicValue.result.isLeft)
          assert(monadicValue.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied])
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic: a satisfied predicate marks the job succeeded") {
    service.eventStream { agent =>
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
          assertEquals(monadicValue.result, Right(6))
          assertEquals(monadicValue.outcomes.size, 3)
          // monadic jobs have no kind
          assertEquals(monadicValue.outcomes.map(_.record.job.kind), List.fill(3)(None))
          assertEquals(monadicValue.outcomes.map(_.record.job.mode), List.fill(3)(BatchMode.Monadic))
          assert(monadicValue.outcomes(1).record.succeeded)
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("sequential: quasiBatch should support rename and predicate") {
    service.eventStream { agent =>
      agent
        .batchLight("light-sequential")
        .sequential("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
        .withPostCondition(_ >= 2)
        .quasiBatch
        .map { state =>
          assertEquals(state.outcomes.size, 3)
          assertEquals(state.outcomes.head.record.job.name, "a")
          assertEquals(state.outcomes.head.record.job.mode, BatchMode.Sequential)
          assertEquals(state.outcomes.head.record.job.kind, Some(BatchKind.Quasi))
          assertEquals(state.outcomes.head.record.succeeded, false)
          assertEquals(state.outcomes(1).record.succeeded, true)
          assertEquals(state.outcomes(2).record.succeeded, true)
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("sequential: completed jobs are ordered by index") {
    service.eventStream { agent =>
      agent
        .batchLight("light-sequential-ordered")
        .sequential("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
        .quasiBatch
        .map { state =>
          assertEquals(state.outcomes.map(_.record.job.index), List(1, 2, 3))
          assertEquals(state.outcomes.map(_.record.job.name), List("a", "b", "c"))
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("sequential: valueBatch should fail when predicate is not satisfied") {
    service.eventStreamR { agent =>
      Resource.eval(
        agent
          .batchLight("light-sequential-value")
          .sequential("a" -> IO(1), "b" -> IO(2))
          .withPostCondition(_ > 1)
          .valueBatch
          .attempt
          .map { outcome =>
            assert(outcome.fold(_.isInstanceOf[PostConditionUnsatisfied], _ => false))
          }
      )
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("sequential: valueBatch should return all values on success") {
    service.eventStream { agent =>
      agent
        .batchLight("light-sequential-value-ok")
        .sequential("a" -> IO(10), "b" -> IO(20), "c" -> IO(30))
        .valueBatch
        .map { bv =>
          assertEquals(bv.outcomes.size, 3)
          assertEquals(bv.result, List(10, 20, 30))
          assertEquals(bv.mode, BatchMode.Sequential)
          assertEquals(bv.outcomes.map(_.record.job.kind), List.fill(3)(Some(BatchKind.Value)))
          assertEquals(bv.outcomes.map(_.record.job.mode), List.fill(3)(BatchMode.Sequential))
          assertEquals(bv.allPassed, true)
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("parallel: parallel(0) should fail fast") {
    service.eventStreamR { agent =>
      Resource.eval(
        agent
          .batchLight("light-parallel-invalid")
          .parallel(0)("a" -> IO(1))
          .quasiBatch
          .attempt
          .map { outcome =>
            assert(outcome.fold(_.getMessage.contains("parallelism must be > 0"), _ => false))
          }
      )
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 3)
    }
  }

  test("parallel: parallel(fas*) should fail fast when empty") {
    service.eventStreamR { agent =>
      Resource.eval(
        agent
          .batchLight("light-parallel-empty")
          .parallel[Int]()
          .quasiBatch
          .attempt
          .map { outcome =>
            assert(outcome.fold(_.getMessage.contains("parallelism must be > 0"), _ => false))
          }
      )
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 3)
    }
  }

  test("parallel: parallel(fas*) should create parallel mode using input size") {
    service.eventStream { agent =>
      agent
        .batchLight("light-parallel-default")
        .parallel("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
        .withPostCondition(_ >= 2)
        .quasiBatch
        .map { state =>
          assertEquals(state.outcomes.size, 3)
          assertEquals(state.outcomes.head.record.job.name, "a")
          assertEquals(state.outcomes.head.record.job.mode, BatchMode.Parallel(3))
          assertEquals(state.outcomes.head.record.job.kind, Some(BatchKind.Quasi))
          assertEquals(state.outcomes.head.record.succeeded, false)
          assertEquals(state.outcomes(1).record.succeeded, true)
          assertEquals(state.outcomes(2).record.succeeded, true)
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("parallel: parallel(parallelism)(fas*) should use explicit parallelism") {
    service.eventStream { agent =>
      agent
        .batchLight("light-parallel-explicit")
        .parallel(1)("a" -> IO(1), "b" -> IO(2))
        .valueBatch
        .map { value =>
          assertEquals(value.outcomes.size, 2)
          assertEquals(value.mode, BatchMode.Parallel(1))
          assertEquals(value.outcomes.map(_.record.job.kind), List.fill(2)(Some(BatchKind.Value)))
          assertEquals(value.outcomes.map(_.record.job.mode), List.fill(2)(BatchMode.Parallel(1)))
          assertEquals(value.result.sum, 3)
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("parallel: completed jobs are ordered by index") {
    service.eventStream { agent =>
      agent
        .batchLight("light-parallel-ordered")
        .parallel(3)("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
        .quasiBatch
        .map { state =>
          assertEquals(state.outcomes.map(_.record.job.index), List(1, 2, 3))
          assertEquals(state.outcomes.map(_.record.job.name), List("a", "b", "c"))
          ()
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("parallel: valueBatch should fail when predicate is not satisfied") {
    service.eventStreamR { agent =>
      Resource.eval(
        agent
          .batchLight("light-parallel-value")
          .parallel(2)("a" -> IO(1), "b" -> IO(2))
          .withPostCondition(_ > 1)
          .valueBatch
          .attempt
          .map { outcome =>
            assert(outcome.fold(_.isInstanceOf[PostConditionUnsatisfied], _ => false))
          }
      )
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("parallel: failed action cancels sibling jobs") {
    var aCompleted = false
    var cCompleted = false

    service.eventStreamR { agent =>
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
            assert(outcome.isLeft)
            assert(!aCompleted)
            assert(!cCompleted)
          }
      )
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }
}
