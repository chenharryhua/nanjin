package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxApplicativeId
import cats.syntax.group.catsSyntaxSemigroup
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{
  BatchKind,
  Job,
  JobHook,
  JobState,
  PostConditionUnsatisfied
}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class BatchMonadicTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("monadic")

  test("1.good") {
    val se = service.eventStreamR { agent =>
      agent
        .batch("good")
        .monadic { job =>
          for {
            _ <- job.pure(1)
            a <- job("a", IO(1))
            _ <- job.pure(2)
            b <- job("b", IO(2))
            _ <- 3.pure[job.Monadic]
            c <- job("c", IO(3))
          } yield a + b + c
        }
        .monadicBatch(JobHook.noop[IO, Json] |+| JobHook(agent.logger).json)
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("2.exception") {
    var completedJob: JobState[Json] = null
    val tracer = JobHook.noop[IO, Json].onComplete { jo =>
      IO { completedJob = jo }
    }
    val se = service.eventStreamR { agent =>
      agent
        .batch("exception")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            b <- job("b", IO.raiseError[Int](new Exception()))
            c <- job("c", IO(3))
          } yield a + b + c
        }
        .monadicBatch(tracer)
        .map { monadicValue =>
          assert(monadicValue.result.isLeft)
          assert(monadicValue.result.left.toOption.get.isInstanceOf[Exception])
        }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    assert(!completedJob.completed.done)
    assert(completedJob.completed.job.index == 2)
  }

  test("3.invincible - exception") {
    var completedJob: List[JobState[Json]] = Nil
    val tracer = JobHook.noop[IO, Json]
      .onComplete(jo => IO { completedJob = jo :: completedJob })
    val se = service.eventStreamR { agent =>
      agent
        .batch("invincible")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            _ <- job.failSafe("b", IO.raiseError[Boolean](new Exception()))
            c <- job("c", IO(3))
          } yield a + c
        }
        .monadicBatch(tracer)
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

    val sorted = completedJob.reverse

    println(sorted)

    assert(sorted.head.completed.done)
    assert(sorted.head.completed.job.index == 1)

    assert(!sorted(1).completed.done)
    assert(sorted(1).completed.job.index == 2)

    assert(sorted(2).completed.done)
    assert(sorted(2).completed.job.index == 3)
  }

  test("4.invincible - false") {
    var completedJob: List[JobState[Json]] = Nil
    val tracer =
      JobHook.noop[IO, Json].onComplete(jo => IO { completedJob = jo :: completedJob })
    val se = service.eventStreamR { agent =>
      agent
        .batch("invincible")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            _ <- job.failSafe("b", IO(false))
            c <- job("c", IO(3))
          } yield a + c
        }
        .monadicBatch(tracer)
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

    val sorted = completedJob.reverse

    assert(sorted.head.completed.done)
    assert(sorted.head.completed.job.index == 1)

    assert(sorted(1).result.isRight)
    assert(!sorted(1).completed.done)
    assert(sorted(1).completed.job.index == 2)

    assert(sorted(2).completed.done)
    assert(sorted(2).completed.job.index == 3)
  }

  test("4a.withFilter on a pure value should fail without crashing") {
    val se = service.eventStreamR { agent =>
      agent
        .batch("filter-pure")
        .monadic { job =>
          job.pure(1).withFilter(_ => false)
        }
        .monadicBatch(JobHook.noop)
        .map { monadicValue =>
          assert(monadicValue.result.isLeft)
          assert(monadicValue.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied])
        }
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("4b.failSafe should emit boolean json to job hook") {
    var completedJob: List[JobState[Json]] = Nil
    val tracer =
      JobHook.noop[IO, Json].onComplete(jo => IO { completedJob = jo :: completedJob })

    val se = service.eventStreamR { agent =>
      agent
        .batch("invincible-json")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            ok <- job.failSafe("b", IO(true))
            ko <- job.failSafe("c", IO(false))
            d <- job("d", IO(4))
          } yield a + d + (if (ok) 10 else 0) + (if (ko) 100 else 0)
        }
        .monadicBatch(tracer)
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

    val sorted = completedJob.reverse

    assert(sorted.size == 4)
    assert(sorted.head.result == Right(Json.fromInt(1)))
    assert(sorted(1).completed.job.kind == BatchKind.Quasi)
    assert(sorted(1).completed.done)
    assert(sorted(1).result == Right(Json.True))
    assert(sorted(2).completed.job.kind == BatchKind.Quasi)
    assert(!sorted(2).completed.done)
    assert(sorted(2).result == Right(Json.False))
    assert(sorted(3).result == Right(Json.fromInt(4)))
  }

  test("4c.failSafe should expose the thrown exception to the hook") {
    val errorMessage = "boom"
    var completedJob: List[JobState[Json]] = Nil
    val tracer =
      JobHook.noop[IO, Json].onComplete(jo => IO { completedJob = jo :: completedJob })

    val se = service.eventStreamR { agent =>
      agent
        .batch("fail-safe-exception")
        .monadic { job =>
          for {
            _ <- job("a", IO(1))
            _ <- job.failSafe("b", IO.raiseError[Boolean](new Exception(errorMessage)))
            _ <- job("c", IO(3))
          } yield ()
        }
        .monadicBatch(tracer)
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

    val sorted = completedJob

    assert(sorted.size == 3)
    assert(sorted(1).completed.job.kind == BatchKind.Quasi)
    assert(!sorted(1).completed.done)
    assert(sorted(1).result.isLeft)
    assert(sorted(1).result.left.toOption.get.getMessage == errorMessage)
  }

  test("5.filter") {
    var completedJob: List[JobState[Json]] = Nil
    val tracer =
      JobHook.noop[IO, Json].onComplete(jo => IO { completedJob = jo :: completedJob })
    val se = service.eventStreamR { agent =>
      agent
        .batch("exception")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            b <- job("b", IO(false))
            if b
            c <- job("c", IO(3))
          } yield a + c
        }
        .monadicBatch(tracer)
        .map { monadicValue =>
          assert(monadicValue.result.isLeft)
          assert(monadicValue.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied])
        }
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    assert(completedJob.size == 2)
    val sorted = completedJob.reverse

    assert(sorted.head.completed.done)
    assert(sorted.head.completed.job.index == 1)
    assert(sorted(1).completed.done)
    assert(sorted(1).completed.job.index == 2)
  }

  test("5b.filter should preserve post-condition failure in job state") {
    var cExecuted = false
    var completedJob: List[JobState[Json]] = Nil
    val tracer = JobHook.noop[IO, Json].onComplete(jo => IO { completedJob = jo :: completedJob })

    val se = service.eventStreamR { agent =>
      agent
        .batch("filter-state")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            b <- job("b", IO(false))
            if b
            c <- job("c", IO { cExecuted = true; 3 })
          } yield a + c
        }
        .monadicBatch(tracer)
        .map { monadicValue =>
          assert(monadicValue.result.isLeft)
          assert(monadicValue.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied])
        }
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    assert(!cExecuted)

    val sorted = completedJob.reverse
    assert(sorted.size == 2)
    assert(sorted.head.result == Right(Json.fromInt(1)))
    assert(sorted(1).result == Right(Json.False))
    assert(sorted(1).completed.done)
    assert(sorted(1).completed.job.index == 2)
  }

  test("6.cancel") {
    var completedJob: List[JobState[Json]] = Nil
    var canceledJob: Job = null
    val tracer = JobHook
      .noop[IO, Json]
      .onCancel(bj => IO { canceledJob = bj }).onComplete(jrv => IO { completedJob = jrv :: completedJob })

    val se = service.eventStream { agent =>
      agent
        .batch("good")
        .monadic { job =>
          for {
            a <- job("a", IO(1).delayBy(1.second))
            b <- job("b", IO(2).delayBy(1.seconds))
            c <- job("c", IO(3).delayBy(2.second))
            d <- job("d", IO(4).delayBy(1.second))
          } yield a + b + c + d
        }
        .monadicBatch(tracer)
        .memoizedAcquire
        .use(_.timeout(3.second))
        .attempt
        .void
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    assert(completedJob.size == 2)
    assert(canceledJob.index == 3)
  }
}
