package mtest.guard

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import com.github.chenharryhua.nanjin.guard.batch.{JobResultError, PostConditionUnsatisfied, TraceJob}

class BatchSequentialSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("sequential")

  private val tracer = TraceJob.noop[IO, Int]

  "quasi" - {
    "good job".in {
      val jobs = List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        agent.batch("good job").sequential(jobs*).quasiBatch(TraceJob.noop)
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }

    "exception".in {
      val jobs =
        List("a" -> IO(1), "b" -> IO.raiseError(new Exception()), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        val result = agent.batch("exception").sequential(jobs*).quasiBatch(TraceJob.noop)
        result.asserting { mb =>
          mb.jobs.head.done.shouldBe(true)
          mb.jobs(1).done.shouldBe(false)
          mb.jobs(2).done.shouldBe(true)
          mb.jobs(3).done.shouldBe(true)
          mb.jobs(4).done.shouldBe(true)
        }
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }

    "predicate".in {
      val jobs =
        List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        val result =
          agent.batch("predicate").sequential(jobs*).withPredicate(_ > 3).quasiBatch(TraceJob.noop)
        result.asserting { mb =>
          mb.jobs.head.done.shouldBe(false)
          mb.jobs(1).done.shouldBe(false)
          mb.jobs(2).done.shouldBe(false)
          mb.jobs(3).done.shouldBe(true)
          mb.jobs(4).done.shouldBe(true)
        }
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }
  }

  "value" - {
    "good job".in {
      val jobs = List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        agent.batch("good job").sequential(jobs*).batchValue(TraceJob.noop)
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }

    "exception".in {
      val jobs =
        List(
          "a" -> IO(1),
          "b" -> IO.raiseError(new Exception("abc")),
          "c" -> IO(3),
          "d" -> IO(4),
          "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        val result = agent
          .batch("exception")
          .sequential(jobs*)
          .batchValue(tracer.map(_.onError { case JobResultError(jo, oc) =>
            IO {
              assert(!jo.done)
              assert(jo.job.index == 2)
              assert(oc.getMessage == "abc")
            }.void
          }))
        result.assertThrowsError[Exception](_.getMessage.shouldBe("abc"))
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }

    "predicate".in {
      val jobs =
        List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        val result =
          agent
            .batch("predicate")
            .sequential(jobs*)
            .withPredicate(_ > 3)
            .batchValue(tracer.map(_.onComplete { jo =>
              IO {
                assert(!jo.resultState.done)
                assert(jo.resultState.job.index == 1)
                assert(jo.value == 1)
              }.void
            }))
        result.assertThrowsError[PostConditionUnsatisfied](_.job.index.shouldBe(1))
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }
  }
}
