package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{JobHook, JobState}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.funsuite.AnyFunSuite

/** Consolidates the invariant that a `JobState`'s completion flag, its derived `succeeded`, and its
  * result agree: `completed.succeeded == succeeded == result.isRight`. Rather than enforcing this with a
  * runtime assertion in the data type, we exercise every path that produces a `JobState` and check that the
  * three views line up.
  */
class JobStateInvariantSpec extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("job-state-invariant")

  /** The three-way invariant a well-formed `JobState` must satisfy. */
  private def check_aligned[A](js: JobState[A]): Unit = {
    assert(js.record.succeeded == js.result.isRight, "completed flag disagrees with result")
    assert(js.succeeded == js.result.isRight, "succeeded disagrees with result")
    assert(js.succeeded == js.record.succeeded, "succeeded disagrees with completed flag")
    ()
  }

  test("quasi parallel - mixed success and exception") {
    val jobs = List(
      "a" -> IO(1),
      "b" -> IO.raiseError[Int](new Exception("boom")),
      "c" -> IO(3))
    val se = service.eventStream { agent =>
      agent
        .batch("quasi.parallel.mixed")
        .parallel(jobs*)
        .quasiBatch(JobHook.noop)
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
        .quasiBatch(JobHook.noop)
        .use(qb => IO(qb.jobs.foreach(check_aligned)))
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("quasi sequential - mixed success and exception") {
    val jobs = List(
      "a" -> IO(1),
      "b" -> IO.raiseError[Int](new Exception("boom")),
      "c" -> IO(3),
      "d" -> IO(4))
    val se = service.eventStream { agent =>
      agent
        .batch("quasi.sequential.mixed")
        .sequential(jobs*)
        .quasiBatch(JobHook.noop)
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
        .quasiBatch(JobHook.noop)
        .use(qb => IO(qb.jobs.foreach(check_aligned)))
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("value sequential - every completed job state is aligned") {
    var completed: List[JobState[Int]] = Nil
    val tracer = JobHook.noop[IO, Int].onComplete(js => IO { completed = js :: completed })
    val jobs = List(
      "a" -> IO(1),
      "b" -> IO.raiseError[Int](new Exception("boom")),
      "c" -> IO(3))
    val se = service.eventStream { agent =>
      agent
        .batch("value.sequential.mixed")
        .sequential(jobs*)
        .valueBatch(tracer)
        .attempt
        .use(e => IO(assert(e.isLeft)))
        .void
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

    assert(completed.nonEmpty)
    completed.foreach(check_aligned)
  }

  test("value parallel - every completed job state is aligned") {
    var completed: List[JobState[Int]] = Nil
    val tracer = JobHook.noop[IO, Int].onComplete(js => IO { completed = js :: completed })
    val jobs = List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
    val se = service.eventStream { agent =>
      agent
        .batch("value.parallel.predicate")
        .parallel(jobs*)
        .withPostCondition(_ < 2)
        .valueBatch(tracer)
        .attempt
        .use(e => IO(assert(e.isLeft)))
        .void
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

    assert(completed.nonEmpty)
    completed.foreach(check_aligned)
  }
}
