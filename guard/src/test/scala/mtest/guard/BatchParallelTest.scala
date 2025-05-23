package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.action.PostConditionUnsatisfied
import com.github.chenharryhua.nanjin.guard.action.{BatchJob, JobResultState, TraceJob}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class BatchParallelTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("parallel")

  test("1.good") {
    val jobs = List("a" -> IO(1), "b" -> IO(2))
    val se = service.eventStreamR { agent =>
      agent.batch("good job").parallel(jobs*).quasiBatch(TraceJob.universal(agent))
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("2.exception - quasi") {
    val jobs = List(
      "a" -> IO(1).delayBy(1.second),
      "b" -> IO(2).delayBy(3.seconds),
      "c" -> IO.raiseError(new Exception()).delayBy(2.seconds))
    val se = service.eventStream { agent =>
      agent.batch("exception.quasi").parallel(jobs*).quasiBatch(TraceJob.universal(agent)).use { mb =>
        IO {
          assert(mb.jobs.head.done)
          assert(mb.jobs(1).done)
          assert(!mb.jobs(2).done)
        }.void
      }
    }.evalTap(console.text[IO]).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("3.exception - value") {
    var errorJob: BatchJob    = null
    var canceledJob: BatchJob = null
    var succJob: BatchJob     = null
    val tracer: TraceJob.GenericTracer[IO, Int] = TraceJob
      .generic[IO, Int]
      .onError((_, jo) => IO { errorJob = jo.job })
      .onCancel(jo => IO { canceledJob = jo })
      .onComplete((_, jo) => IO { succJob = jo.job })
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
    }.evalTap(console.text[IO]).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

    assert(succJob.index == 1)
    assert(canceledJob.index == 2)
    assert(errorJob.index == 3)
  }

  test("4.predicate - quasi") {
    val jobs =
      List("a" -> IO(1).delayBy(1.second), "b" -> IO(2).delayBy(3.seconds), "c" -> IO(3).delayBy(2.seconds))
    val se = service.eventStream { agent =>
      agent
        .batch("predicate.quasi")
        .parallel(jobs*)
        .withPredicate(_ > 2)
        .quasiBatch(TraceJob.universal(agent))
        .use { mb =>
          IO {
            assert(!mb.jobs.head.done)
            assert(!mb.jobs(1).done)
            assert(mb.jobs(2).done)
          }.void
        }
    }.evalTap(console.text[IO]).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("5.predicate - value") {
    var canceledJob: BatchJob              = null
    var completedJob: List[JobResultState] = Nil
    val tracer = TraceJob
      .generic[IO, Int]
      .onCancel(jo => IO { canceledJob = jo })
      .onComplete((_, jo) => IO { completedJob = jo :: completedJob })
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
    }.evalTap(console.text[IO]).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

    val sorted = completedJob.sortBy(_.job.index)

    assert(sorted.head.job.index == 1)
    assert(sorted.head.done)

    assert(sorted(1).job.index == 2)
    assert(!sorted(1).done)

    assert(canceledJob.index == 3)
  }

}
