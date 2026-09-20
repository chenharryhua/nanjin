package mtest.guard

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.JobState
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import munit.CatsEffectSuite

/** Checks the invariant a well-formed `JobState` must satisfy:
  *
  *   - `js.record.succeeded` implies `js.result.isRight` — a job is only recorded as succeeded when its
  *     effect produced a value. The converse does not hold: a quasi job whose effect succeeds but whose
  *     post-condition rejects the value keeps the value (`result.isRight`) yet records `succeeded = false`.
  *
  * Rather than enforcing this with a runtime assertion in the data type, we exercise every path that produces
  * a `JobState` and check the implication holds.
  */
class JobStateInvariantSpec extends CatsEffectSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("job-state-invariant")

  /** The invariant a well-formed `JobState` must satisfy. */
  private def check_aligned[A](js: JobState[A]): Unit = {
    // a recorded success implies the effect produced a value; a predicate rejection keeps the value but
    // records failure, so the reverse implication need not hold.
    assert(!js.record.succeeded || js.result.isRight, "recorded success without a result value")
    ()
  }

  test("1.quasi parallel - mixed success and exception") {
    val jobs = List("a" -> IO(1), "b" -> IO.raiseError[Int](new Exception("boom")), "c" -> IO(3))
    service.eventStream { agent =>
      agent
        .batch("quasi.parallel.mixed")
        .parallel(jobs*)
        .quasiBatch
        .use(qb => IO(qb.outcomes.foreach(check_aligned)))
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("2.quasi parallel - post-condition failure") {
    val jobs = List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
    service.eventStream { agent =>
      agent
        .batch("quasi.parallel.predicate")
        .parallel(jobs*)
        .withPostCondition(_ > 2)
        .quasiBatch
        .use(qb => IO(qb.outcomes.foreach(check_aligned)))
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("3.quasi sequential - mixed success and exception") {
    val jobs =
      List("a" -> IO(1), "b" -> IO.raiseError[Int](new Exception("boom")), "c" -> IO(3), "d" -> IO(4))
    service.eventStream { agent =>
      agent
        .batch("quasi.sequential.mixed")
        .sequential(jobs*)
        .quasiBatch
        .use(qb => IO(qb.outcomes.foreach(check_aligned)))
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("4.quasi sequential - post-condition failure") {
    val jobs = List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4))
    service.eventStream { agent =>
      agent
        .batch("quasi.sequential.predicate")
        .sequential(jobs*)
        .withPostCondition(_ > 3)
        .quasiBatch
        .use(qb => IO(qb.outcomes.foreach(check_aligned)))
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("5.sequential - every completed job state is aligned") {
    val jobs = List("a" -> IO(1), "b" -> IO.raiseError[Int](new Exception("boom")), "c" -> IO(3))
    service.eventStream { agent =>
      agent
        .batch("value.sequential.mixed")
        .sequential(jobs*)
        .quasiBatch
        .use { qb =>
          IO {
            assert(qb.outcomes.nonEmpty)
            qb.outcomes.foreach(check_aligned)
          }
        }
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("6.parallel - every completed job state is aligned") {
    val jobs = List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
    service.eventStream { agent =>
      agent
        .batch("value.parallel.predicate")
        .parallel(jobs*)
        .withPostCondition(_ < 2)
        .quasiBatch
        .use { qb =>
          IO {
            assert(qb.outcomes.nonEmpty)
            qb.outcomes.foreach(check_aligned)
          }
        }
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }
}
