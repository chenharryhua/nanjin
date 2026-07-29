package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{
  BatchKind,
  BatchMode,
  Job,
  JobHook,
  JobState,
  PostConditionUnsatisfied
}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class BatchParallelTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("parallel")

  test("1.invalid parallelism should fail fast") {
    val se = service.eventStream { agent =>
      agent
        .batch("invalid.parallelism")
        .parallel(0)("a" -> IO(1))
        .quasiBatch(JobHook.noop)
        .use_
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 3)
  }

  test("2.good") {
    val jobs = List("a" -> IO(1), "b" -> IO(2))
    val se = service.eventStreamR { agent =>
      agent.batch("good job").parallel(jobs*).quasiBatch(JobHook(agent.logger).standard)
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("3.exception - quasi") {
    val jobs = List(
      "a" -> IO(1).delayBy(1.second),
      "b" -> IO(2).delayBy(3.seconds),
      "c" -> IO.raiseError(new Exception()).delayBy(2.seconds))
    val se = service.eventStream { agent =>
      agent
        .batch("exception.quasi")
        .parallel(jobs*)
        .quasiBatch(JobHook(agent.heraldLogger).standard)
        .use { mb =>
          IO {
            assert(mb.jobs.head.completed.done)
            assert(mb.jobs(1).completed.done)
            assert(!mb.jobs(2).completed.done)
          }.void
        }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("4.exception - value") {
    var errorJob: Job = null
    var canceledJob: Job = null
    var succJob: Job = null
    val tracer: JobHook.Bridge[IO, Int] =
      JobHook.noop[IO, Int]
        .onCancel(jo => IO { canceledJob = jo })
        .onComplete(jo =>
          IO {
            if (jo.result.isLeft) errorJob = jo.completed.job
            else succJob = jo.completed.job
          })
    val jobs = List(
      "a" -> IO(1).delayBy(1.second),
      "b" -> IO(2).delayBy(3.seconds),
      "c" -> IO.raiseError(new Exception()).delayBy(2.seconds))
    val se = service.eventStream { agent =>
      agent
        .batch("exception.value")
        .parallel(jobs*)
        .batchValue(tracer)
        .attempt
        .use(e => IO(assert(e.isLeft)))
        .void
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

    assert(succJob.index == 1)
    assert(canceledJob.index == 2)
    assert(errorJob.index == 3)
  }

  test("5.predicate - quasi") {
    val jobs =
      List("a" -> IO(1).delayBy(1.second), "b" -> IO(2).delayBy(3.seconds), "c" -> IO(3).delayBy(2.seconds))
    val se = service.eventStream { agent =>
      agent
        .batch("predicate.quasi")
        .parallel(jobs*)
        .withPredicate(_ > 2)
        .quasiBatch(JobHook(agent.logger).standard[Int])
        .use { mb =>
          IO {
            assert(!mb.jobs.head.completed.done)
            assert(mb.jobs.head.completed.job.mode === BatchMode.Parallel(3))
            assert(mb.jobs.head.completed.job.kind === BatchKind.Quasi)
            assert(!mb.jobs(1).completed.done)
            assert(mb.jobs(2).completed.done)
          }.void
        }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("6.predicate - value") {
    var canceledJob: Job = null
    var completedJob: List[JobState[Int]] = Nil
    val tracer = JobHook
      .noop[IO, Int]
      .onCancel(jo => IO { canceledJob = jo }).onComplete(jo => IO { completedJob = jo :: completedJob })
    val jobs =
      List("a" -> IO(1).delayBy(1.second), "b" -> IO(2).delayBy(2.seconds), "c" -> IO(3).delayBy(3.seconds))
    val se = service.eventStream { agent =>
      agent
        .batch("predicate.value")
        .parallel(jobs*)
        .withPredicate(_ < 2)
        .batchValue(tracer)
        .attempt
        .use(e => IO(assert(e.fold(_.isInstanceOf[PostConditionUnsatisfied], _ => false))))
        .void
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

    val sorted = completedJob.sortBy(_.completed.job.index)

    assert(sorted.head.completed.job.index == 1)
    assert(sorted.head.completed.job.kind === BatchKind.Value)
    assert(sorted.head.completed.job.mode === BatchMode.Parallel(3))
    assert(sorted.head.completed.done)

    assert(sorted(1).completed.job.index == 2)
    assert(!sorted(1).completed.done)

    assert(canceledJob.index == 3)
  }

  test("7.failed action cancels sibling jobs") {
    var canceledJobs: List[Job] = Nil
    var completedJob: List[JobState[Int]] = Nil
    val tracer = JobHook
      .noop[IO, Int]
      .onCancel(jo => IO { canceledJobs = jo :: canceledJobs })
      .onComplete(jo => IO { completedJob = jo :: completedJob })

    val jobs = List(
      "a" -> IO(1).delayBy(1.second),
      "b" -> IO.raiseError(new Exception("boom")).delayBy(1.second),
      "c" -> IO(3).delayBy(3.seconds)
    )

    val se = service.eventStream { agent =>
      agent
        .batch("failed-cancels-siblings")
        .parallel(jobs*)
        .batchValue(tracer)
        .attempt
        .use(e => IO(assert(e.isLeft)))
        .void
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    assert(canceledJobs.exists(_.index == 3))
    assert(canceledJobs.nonEmpty)

    val sorted = completedJob.sortBy(_.completed.job.index)
    assert(sorted.nonEmpty)
    assert(sorted.head.result.isRight)
    assert(sorted.exists(_.result.isLeft))
    assert(sorted.exists(_.completed.done == false))
  }

}
