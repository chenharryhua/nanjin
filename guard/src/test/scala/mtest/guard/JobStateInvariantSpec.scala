package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.JobState
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.funsuite.AnyFunSuite

/** Consolidates the invariants a well-formed `JobState` must satisfy.
  *
  *   - `js.succeeded == js.record.succeeded` — job success is the recorded (post-condition) outcome, not
  *     merely whether the effect produced a value.
  *   - `js.record.succeeded` implies `js.result.isRight` — a job is only recorded as succeeded when its
  *     effect produced a value. The converse does not hold: a quasi job whose effect succeeds but whose
  *     post-condition rejects the value keeps the value (`result.isRight`) yet records `succeeded = false`.
  *
  * Rather than enforcing these with a runtime assertion in the data type, we exercise every path that
  * produces a `JobState` and check that the views line up.
  */
class JobStateInvariantSpec extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("job-state-invariant")

  /** The invariants a well-formed `JobState` must satisfy. */
  private def check_aligned[A](js: JobState[A]): Unit = {
    assert(js.succeeded == js.record.succeeded, "succeeded disagrees with the recorded outcome")
    // a recorded success implies the effect produced a value; a predicate rejection keeps the value but
    // records failure, so the reverse implication need not hold.
    assert(!js.record.succeeded || js.result.isRight, "recorded success without a result value")
    ()
  }

  test("quasi parallel - mixed success and exception") {
    val jobs = List("a" -> IO(1), "b" -> IO.raiseError[Int](new Exception("boom")), "c" -> IO(3))
    val se = service.eventStream { agent =>
      agent
        .batch("quasi.parallel.mixed")
        .parallel(jobs*)
        .quasiBatch
        .use(qb => IO(qb.jobs.foreach(check_aligned)))
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("quasi parallel - post-condition failure") {
    val jobs = List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
    val se = service.eventStream { agent =>
      agent
        .batch("quasi.parallel.predicate")
        .parallel(jobs*)
        .withPostCondition(_ > 2)
        .quasiBatch
        .use(qb => IO(qb.jobs.foreach(check_aligned)))
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("quasi sequential - mixed success and exception") {
    val jobs =
      List("a" -> IO(1), "b" -> IO.raiseError[Int](new Exception("boom")), "c" -> IO(3), "d" -> IO(4))
    val se = service.eventStream { agent =>
      agent
        .batch("quasi.sequential.mixed")
        .sequential(jobs*)
        .quasiBatch
        .use(qb => IO(qb.jobs.foreach(check_aligned)))
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("quasi sequential - post-condition failure") {
    val jobs = List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4))
    val se = service.eventStream { agent =>
      agent
        .batch("quasi.sequential.predicate")
        .sequential(jobs*)
        .withPostCondition(_ > 3)
        .quasiBatch
        .use(qb => IO(qb.jobs.foreach(check_aligned)))
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("sequential - every completed job state is aligned") {
    val jobs = List("a" -> IO(1), "b" -> IO.raiseError[Int](new Exception("boom")), "c" -> IO(3))
    val se = service.eventStream { agent =>
      agent
        .batch("value.sequential.mixed")
        .sequential(jobs*)
        .quasiBatch
        .use { qb =>
          IO {
            assert(qb.jobs.nonEmpty)
            qb.jobs.foreach(check_aligned)
          }
        }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("parallel - every completed job state is aligned") {
    val jobs = List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
    val se = service.eventStream { agent =>
      agent
        .batch("value.parallel.predicate")
        .parallel(jobs*)
        .withPostCondition(_ < 2)
        .quasiBatch
        .use { qb =>
          IO {
            assert(qb.jobs.nonEmpty)
            qb.jobs.foreach(check_aligned)
          }
        }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }
}
