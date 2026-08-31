package mtest.guard

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits.catsSyntaxApplicativeByName
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{
  BatchKind,
  BatchMode,
  BatchValue,
  JobHook,
  PostConditionUnsatisfied
}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class BatchSequentialSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("sequential")

  private val tracer = JobHook.noop[IO, Int]

  "quasi" - {
    "good job".in {
      val jobs = List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        agent.batch("good job").sequential(jobs*).quasiBatch(JobHook.noop)
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }

    "exception".in {
      val jobs =
        List("a" -> IO(1), "b" -> IO.raiseError(new Exception()), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        val result = agent.batch("exception").sequential(jobs*).quasiBatch(JobHook.noop)
        result.asserting { mb =>
          mb.jobs.head.completed.succeeded.shouldBe(true)
          mb.jobs.head.completed.job.mode.shouldBe(BatchMode.Sequential)
          mb.jobs.head.completed.job.kind.shouldBe(BatchKind.Quasi)
          mb.jobs(1).completed.succeeded.shouldBe(false)
          mb.jobs(2).completed.succeeded.shouldBe(true)
          mb.jobs(3).completed.succeeded.shouldBe(true)
          mb.jobs(4).completed.succeeded.shouldBe(true)
        }
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }

    "predicate".in {
      val jobs =
        List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        val result =
          agent.batch("predicate").sequential(jobs*).withPostCondition(_ > 3).quasiBatch(JobHook.noop)
        result.asserting { mb =>
          mb.jobs.head.completed.succeeded.shouldBe(false)
          mb.jobs(1).completed.succeeded.shouldBe(false)
          mb.jobs(2).completed.succeeded.shouldBe(false)
          mb.jobs(3).completed.succeeded.shouldBe(true)
          mb.jobs(4).completed.succeeded.shouldBe(true)
        }
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }
  }

  "value" - {
    "good job".in {
      val jobs = List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        agent.batch("good job").sequential(jobs*).batchValue(JobHook.noop)
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
          .batchValue(tracer.onComplete { jo =>
            IO {
              assert(jo.result.isLeft)
              assert(!jo.completed.succeeded)
              assert(jo.result.left.toOption.get.getMessage == "abc")
            }.whenA(jo.completed.job.index == 2)
          })
        result.assertThrowsError[Exception](_.getMessage.shouldBe("abc"))
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }

    "predicate".in {
      val jobs =
        List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        val result: Resource[IO, BatchValue[Int]] =
          agent
            .batch("predicate")
            .sequential(jobs*)
            .withPostCondition(_ > 3)
            .batchValue(tracer.onComplete { jo =>
              IO {
                assert(!jo.completed.succeeded)
                assert(jo.completed.job.index == 1)
              }.void
            })
        result.assertThrowsError[PostConditionUnsatisfied](_.job.map(_.index).shouldBe(Some(1)))
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }
  }
}
