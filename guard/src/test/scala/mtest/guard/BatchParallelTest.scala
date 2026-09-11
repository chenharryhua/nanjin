package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{BatchKind, BatchMode, PostConditionUnsatisfied}
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
        .quasiBatch
        .use_
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 3)
  }

  test("2.good") {
    val jobs = List("a" -> IO(1), "b" -> IO(2))
    val se = service.eventStreamR { agent =>
      agent.batch("good job").parallel(jobs*).quasiBatch
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
        .quasiBatch
        .use { mb =>
          IO {
            assert(mb.jobs.head.record.succeeded)
            assert(mb.jobs(1).record.succeeded)
            assert(!mb.jobs(2).record.succeeded)
          }.void
        }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("4.exception - value") {
    val jobs = List(
      "a" -> IO(1).delayBy(1.second),
      "b" -> IO(2).delayBy(3.seconds),
      "c" -> IO.raiseError(new Exception()).delayBy(2.seconds))
    val se = service.eventStream { agent =>
      agent
        .batch("exception.value")
        .parallel(jobs*)
        .valueBatch
        .attempt
        .use(e => IO(assert(e.isLeft)))
        .void
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("5.predicate - quasi") {
    val jobs =
      List("a" -> IO(1).delayBy(1.second), "b" -> IO(2).delayBy(3.seconds), "c" -> IO(3).delayBy(2.seconds))
    val se = service.eventStream { agent =>
      agent
        .batch("predicate.quasi")
        .parallel(jobs*)
        .withPostCondition(_ > 2)
        .quasiBatch
        .use { mb =>
          IO {
            assert(!mb.jobs.head.record.succeeded)
            assert(mb.jobs.head.record.job.mode === BatchMode.Parallel(3))
            assert(mb.jobs.head.record.job.kind === Some(BatchKind.Quasi))
            assert(!mb.jobs(1).record.succeeded)
            assert(mb.jobs(2).record.succeeded)
          }.void
        }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("6.predicate - value") {
    val jobs =
      List("a" -> IO(1).delayBy(1.second), "b" -> IO(2).delayBy(2.seconds), "c" -> IO(3).delayBy(3.seconds))
    val se = service.eventStream { agent =>
      agent
        .batch("predicate.value")
        .parallel(jobs*)
        .withPostCondition(_ < 2)
        .valueBatch
        .attempt
        .use(e => IO(assert(e.fold(_.isInstanceOf[PostConditionUnsatisfied], _ => false))))
        .void
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("7.failed action cancels sibling jobs") {
    val jobs = List(
      "a" -> IO(1).delayBy(1.second),
      "b" -> IO.raiseError(new Exception("boom")).delayBy(2.second),
      "c" -> IO(3).delayBy(3.seconds)
    )

    val se = service.eventStream { agent =>
      agent
        .batch("failed-cancels-siblings")
        .parallel(jobs*)
        .valueBatch
        .attempt
        .use(e => IO(assert(e.isLeft)))
        .void
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

}
