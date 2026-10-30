package mtest.guard

import cats.Applicative
import cats.effect.{IO, Resource}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{
  BatchMode,
  PostConditionUnsatisfied,
  UntrackedStepException
}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import munit.CatsEffectSuite
import org.typelevel.otel4s.trace.{Span, TracerProvider}

/** Coverage of the traced-batch path (`agent.batchTraced`). The default service uses `TracerProvider.noop`,
  * so spans are created with no backend and no network; the `withTracerProvider` call below (also noop)
  * exercises `ServiceConfigF.withTracerProvider` without changing that. This suite targets the parallel
  * runner and the span-aware/pure/untracked monadic builders, which the other batch suites do not reach. The
  * traced `quasiBatch`/`valueBatch`/`monadicBatch` all return `F[_]` directly (no `Resource`), unlike the
  * metered façade. Production source is not modified.
  */
class BatchTracedTest extends CatsEffectSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch")
      .service("traced")
      .updateConfig(_.withTracerProvider(Resource.pure(TracerProvider.noop[IO])))

  test("1.parallel quasiBatch with post-condition records every outcome") {
    service.eventStream { agent =>
      agent
        .batchTraced("traced.parallel.quasi", _.build)
        .parallel(2)("a" -> ((_: Span[IO]) => IO(1)), "b" -> ((_: Span[IO]) => IO(2)))
        .withPostCondition(_ > 0)
        .quasiBatch
        .map { qb =>
          assertEquals(qb.mode, BatchMode.Parallel(2))
          assertEquals(qb.outcomes.size, 2)
          assert(qb.outcomes.forall(_.succeeded))
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("2.parallel valueBatch returns successful values") {
    service.eventStream { agent =>
      agent
        .batchTraced("traced.parallel.value", _.build)
        .parallel(2)("a" -> ((_: Span[IO]) => IO(1)), "b" -> ((_: Span[IO]) => IO(2)))
        .valueBatch
        .map(vb => assertEquals(vb.result.sorted, List(1, 2)))
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("3.parallel valueBatch raises when a post-condition is unmet") {
    service.eventStream { agent =>
      agent
        .batchTraced("traced.parallel.value.fail", _.build)
        .parallel(2)("a" -> ((_: Span[IO]) => IO(1)), "b" -> ((_: Span[IO]) => IO(2)))
        .withPostCondition(_ > 5)
        .valueBatch
        .attempt
        .map(e => assert(e.fold(_.isInstanceOf[PostConditionUnsatisfied], _ => false)))
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("4.parallel(0) fails fast with IllegalArgumentException") {
    // `parallel` validates eagerly via `require`, so constructing the batch inside the running agent throws
    // before any job runs; the thrown error surfaces as a non-zero service exit.
    service.eventStream { agent =>
      IO {
        agent
          .batchTraced("traced.parallel.zero", _.build)
          .parallel(0)("a" -> ((_: Span[IO]) => IO(1)))
      }.attempt.map(e => assert(e.left.toOption.exists(_.isInstanceOf[IllegalArgumentException]))).void
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("5.parallel with no jobs defaults concurrency to one") {
    service.eventStream { agent =>
      agent
        .batchTraced("traced.parallel.empty", _.build)
        .parallel[Int]()
        .quasiBatch
        .map { qb =>
          assertEquals(qb.mode, BatchMode.Parallel(1))
          assert(qb.outcomes.isEmpty)
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("6.monadic composes pure, span-aware, and untracked steps") {
    service.eventStream { agent =>
      agent
        .batchTraced("traced.monadic", _.build)
        .monadic { job =>
          for {
            _ <- job.pure(1)
            a <- job("a", (_: Span[IO]) => IO(1))
            _ <- job.untracked(IO.unit)
            b <- job("b", IO(2))
            if b > 0
          } yield a + b
        }
        .monadicBatch
        .map { mb =>
          assertEquals(mb.result, Right(3))
          // pure and untracked steps are invisible; only the two named jobs are recorded
          assertEquals(mb.outcomes.map(_.record.job.name), List("a", "b"))
          assert(mb.outcomes.forall(_.record.job.mode == BatchMode.Monadic))
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("7.monadic untracked failure is wrapped in UntrackedStepException") {
    service.eventStream { agent =>
      agent
        .batchTraced("traced.monadic.untracked.fail", _.build)
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            _ <- job.untracked(IO.raiseError[Int](new Exception("boom")))
            b <- job("b", IO(2))
          } yield a + b
        }
        .monadicBatch
        .map { mb =>
          assert(mb.result.isLeft)
          assert(mb.result.left.toOption.get.isInstanceOf[UntrackedStepException])
          // the untracked step is invisible and the chain short-circuits before "b"
          assertEquals(mb.outcomes.map(_.record.job.name), List("a"))
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("8.monadic withFilter after a failure keeps the chain failed") {
    service.eventStream { agent =>
      agent
        .batchTraced("traced.monadic.filter.after.fail", _.build)
        .monadic { job =>
          for {
            a <- job("a", IO.raiseError[Int](new Exception("boom")))
            if a > 0
          } yield a
        }
        .monadicBatch
        .map { mb =>
          // the chain already failed before the filter, so withFilter leaves it unchanged
          assert(mb.result.isLeft)
          assert(!mb.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied])
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("9.Applicative[Monadic] combines independent jobs via mapN") {
    service.eventStream { agent =>
      agent
        .batchTraced("traced.monadic.applicative", _.build)
        .monadic { job =>
          val A: Applicative[job.Monadic] = summon[Applicative[job.Monadic]]
          A.map2(job("a", IO(1)), job("b", IO(2)))(_ + _)
        }
        .monadicBatch
        .map { mb =>
          assertEquals(mb.result, Right(3))
          assertEquals(mb.outcomes.map(_.record.job.name), List("a", "b"))
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("10.monadic pure-only chain runs no named jobs") {
    service.eventStream { agent =>
      agent
        .batchTraced("traced.monadic.pure", _.build)
        .monadic(job => job.pure(42))
        .monadicBatch
        .map { mb =>
          assertEquals(mb.result, Right(42))
          assert(mb.outcomes.isEmpty)
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("11.sequential quasiBatch over a mix of success and failure") {
    service.eventStream { agent =>
      agent
        .batchTraced("traced.sequential", _.build)
        .sequential(
          "a" -> ((_: Span[IO]) => IO(1)),
          "b" -> ((_: Span[IO]) => IO.raiseError[Int](new Exception("boom"))))
        .quasiBatch
        .map { qb =>
          assertEquals(qb.mode, BatchMode.Sequential)
          assert(qb.outcomes.head.succeeded)
          assert(!qb.outcomes(1).succeeded)
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }
}
