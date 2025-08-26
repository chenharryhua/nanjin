package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxSemigroup
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{
  BatchJob,
  JobHandler,
  JobResultState,
  JobResultValue,
  PostConditionUnsatisfied,
  TraceJob
}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.Json
import io.circe.syntax.EncoderOps
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
            a <- job("a" -> IO(1))
            b <- job("b" -> IO(2))
            c <- job.customise("c" -> IO(3))((a, _) => a.asJson)
          } yield a + b + c
        }
        .batchValue(TraceJob(agent).disableSuccess.disableFailure.disableKickoff.json)
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("2.exception") {
    var completedJob: JobResultState = null
    var errorJob: JobResultState = null
    val tracer: TraceJob[IO, Json] = TraceJob
      .noop[IO, Json]
      .onComplete(jo => IO { completedJob = jo.resultState })
      .onError(jo => IO { errorJob = jo.resultState })
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
        .batchValue(tracer |+| TraceJob(agent).json)
        .attempt
      res.map(r => assert(r.fold(_.isInstanceOf[Exception], _ => false)))

    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    assert(completedJob.done)
    assert(completedJob.job.index == 1)
    assert(!errorJob.done)
    assert(errorJob.job.index == 2)

  }

  test("3.invincible - exception") {
    var completedJob: List[JobResultState] = Nil
    var errorJob: JobResultState = null
    val tracer: TraceJob[IO, Json] = TraceJob
      .noop[IO, Json]
      .onComplete(jo => IO { completedJob = jo.resultState :: completedJob })
      .onError(jo => IO { errorJob = jo.resultState })
    val se = service.eventStreamR { agent =>
      agent
        .batch("invincible")
        .monadic { job =>
          for {
            a <- job("a" -> IO(1))
            _ <- job.failSafe("b", IO.raiseError[Int](new Exception()))(new JobHandler[Int] {
              override def predicate(a: Int): Boolean = true
              override def translate(a: Int, jrs: JobResultState): Json = a.asJson
            })
            c <- job("c" -> IO(3))
          } yield a + c
        }
        .batchValue(tracer |+| TraceJob(agent).json)
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

    val sorted = completedJob.sortBy(_.job.index)

    assert(sorted.head.done)
    assert(sorted.head.job.index == 1)

    assert(errorJob.fail)
    assert(errorJob.job.index == 2)

    assert(sorted(1).done)
    assert(sorted(1).job.index == 3)
  }

  test("4.invincible - false") {
    var completedJob: List[JobResultState] = Nil
    val tracer: TraceJob[IO, Json] =
      TraceJob.noop[IO, Json].onComplete(jo => IO { completedJob = jo.resultState :: completedJob })
    val se = service.eventStreamR { agent =>
      agent
        .batch("invincible")
        .monadic { job =>
          for {
            a <- job("a" -> IO(1))
            _ <- job.failSafe("b" -> IO(10))(new JobHandler[Int] {
              override def predicate(a: Int): Boolean = a > 15
              override def translate(a: Int, jrs: JobResultState): Json = a.asJson
            })
            c <- job("c" -> IO(3))
          } yield a + c
        }
        .batchValue(tracer |+| TraceJob(agent).json)
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

    val sorted = completedJob.sortBy(_.job.index)

    assert(sorted.head.done)
    assert(sorted.head.job.index == 1)

    assert(sorted(1).fail)
    assert(sorted(1).job.index == 2)

    assert(sorted(2).done)
    assert(sorted(2).job.index == 3)
  }

  test("5. filter") {
    var completedJob: List[JobResultState] = Nil
    val tracer: TraceJob[IO, Json] =
      TraceJob.noop[IO, Json].onComplete(jo => IO { completedJob = jo.resultState :: completedJob })
    val se = service.eventStreamR { agent =>
      val res = agent
        .batch("exception")
        .monadic { job =>
          for {
            a <- job("a" -> IO(1))
            b <- job("b" -> IO(false))
            if b
            c <- job("c" -> IO(3))
          } yield a + c
        }
        .batchValue(tracer |+| TraceJob(agent).json)
        .attempt
      res.map(r => assert(r.fold(_.isInstanceOf[PostConditionUnsatisfied], _ => false)))

    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    assert(completedJob.size == 2)
    val sorted = completedJob.sortBy(_.job.index)

    assert(sorted.head.done)
    assert(sorted.head.job.index == 1)
    assert(sorted(1).done)
    assert(sorted(1).job.index == 2)
  }

  test("6.cancel") {
    var completedJob: List[JobResultValue[Json]] = Nil
    var canceledJob: BatchJob = null
    val tracer = TraceJob
      .noop[IO, Json]
      .onCancel(bj => IO { canceledJob = bj })
      .onComplete(jrv => IO { completedJob = jrv :: completedJob })

    val se = service.eventStream { agent =>
      agent
        .batch("good")
        .monadic { job =>
          for {
            a <- job("a" -> IO(1).delayBy(1.second))
            b <- job("b" -> IO(2).delayBy(1.seconds))
            c <- job("c" -> IO(3).delayBy(2.second))
            d <- job("d" -> IO(4).delayBy(1.second))
          } yield a + b + c + d
        }
        .batchValue(tracer)
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
