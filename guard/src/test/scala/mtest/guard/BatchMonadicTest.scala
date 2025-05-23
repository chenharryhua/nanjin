package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxSemigroup
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.action.{JobResultState, TraceJob}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

class BatchMonadicTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("monadic")

  test("1.good") {
    val se = service.eventStreamR { agent =>
      agent
        .batch("good")
        .monadic { job =>
          for {
            a <- job("a" -> IO(1))
            b <- job("b" -> IO(2))
            c <- job("c" -> IO(3))
          } yield a + b + c
        }
        .batchValue(TraceJob(agent).standard)
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("2.exception") {
    var completedJob: JobResultState = null
    var errorJob: JobResultState     = null
    val tracer = TraceJob
      .generic[IO, Json]
      .onComplete((_, jo) => IO { completedJob = jo })
      .onError((_, jo) => IO { errorJob = jo })
    val se = service.eventStreamR { agent =>
      val res = agent
        .batch("exception")
        .monadic { job =>
          for {
            a <- job("a" -> IO(1))
            b <- job("b" -> IO.raiseError[Int](new Exception()))
            c <- job("c" -> IO(3))
          } yield a + b + c
        }
        .batchValue(tracer)
        .attempt
      res.map(r => assert(r.fold(_.isInstanceOf[Exception], _ => false)))

    }.evalTap(console.text[IO]).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    assert(completedJob.done)
    assert(completedJob.job.index == 1)
    assert(!errorJob.done)
    assert(errorJob.job.index == 2)

  }

  test("3.invincible - exception") {
    var completedJob: List[JobResultState] = Nil
    var errorJob: JobResultState           = null
    val tracer: TraceJob[IO, Json] = TraceJob
      .generic[IO, Json]
      .onComplete((_, jo) => IO { completedJob = jo :: completedJob })
      .onError((_, jo) => IO { errorJob = jo })
    val se = service.eventStreamR { agent =>
      agent
        .batch("invincible")
        .monadic { job =>
          for {
            a <- job("a" -> IO(1))
            _ <- job.invincible("b" -> IO.raiseError[Boolean](new Exception()))
            c <- job("c" -> IO(3))
          } yield a + c
        }
        .batchValue(tracer |+| TraceJob(agent).standard)
    }.evalTap(console.text[IO]).compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

    val sorted = completedJob.sortBy(_.job.index)

    assert(sorted.head.done)
    assert(sorted.head.job.index == 1)

    assert(!errorJob.done)
    assert(errorJob.job.index == 2)

    assert(sorted(1).done)
    assert(sorted(1).job.index == 3)
  }

  test("4.invincible - false") {
    var completedJob: List[JobResultState] = Nil
    val tracer: TraceJob[IO, Json] =
      TraceJob.generic[IO, Json].onComplete((_, jo) => IO { completedJob = jo :: completedJob })
    val se = service.eventStreamR { agent =>
      agent
        .batch("invincible")
        .monadic { job =>
          for {
            a <- job("a" -> IO(1))
            _ <- job.invincible("b" -> IO(false))
            c <- job("c" -> IO(3))
          } yield a + c
        }
        .batchValue(tracer |+| TraceJob(agent).standard)
    }.evalTap(console.text[IO]).compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

    val sorted = completedJob.sortBy(_.job.index)

    assert(sorted.head.done)
    assert(sorted.head.job.index == 1)

    assert(!sorted(1).done)
    assert(sorted(1).job.index == 2)

    assert(sorted(2).done)
    assert(sorted(2).job.index == 3)
  }

}
