package mtest.guard

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{BatchKind, BatchMode, PostConditionUnsatisfied, ValueBatch}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class BatchSequentialSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("sequential")

  "quasi" - {
    "good job".in {
      val jobs = List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        agent.batch("good job").sequential(jobs*).quasiBatch
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }

    "exception".in {
      val jobs =
        List("a" -> IO(1), "b" -> IO.raiseError(new Exception()), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        val result = agent.batch("exception").sequential(jobs*).quasiBatch
        result.asserting { mb =>
          mb.jobs.head.record.succeeded.shouldBe(true)
          mb.jobs.head.record.job.mode.shouldBe(BatchMode.Sequential)
          mb.jobs.head.record.job.kind.shouldBe(Some(BatchKind.Quasi))
          mb.jobs(1).record.succeeded.shouldBe(false)
          mb.jobs(2).record.succeeded.shouldBe(true)
          mb.jobs(3).record.succeeded.shouldBe(true)
          mb.jobs(4).record.succeeded.shouldBe(true)
        }
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }

    "predicate".in {
      val jobs =
        List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        val result =
          agent.batch("predicate").sequential(jobs*).withPostCondition(_ > 3).quasiBatch
        result.asserting { mb =>
          mb.jobs.head.record.succeeded.shouldBe(false)
          mb.jobs(1).record.succeeded.shouldBe(false)
          mb.jobs(2).record.succeeded.shouldBe(false)
          mb.jobs(3).record.succeeded.shouldBe(true)
          mb.jobs(4).record.succeeded.shouldBe(true)
        }
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }
  }

  "value" - {
    "good job".in {
      val jobs = List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        agent
          .batch("good job")
          .sequential(jobs*)
          .valueBatch
          .evalTap { bv =>
            IO {
              bv.jobs.map(_.record.job.kind).shouldBe(List.fill(5)(Some(BatchKind.Value)))
              bv.jobs.map(_.record.job.mode).shouldBe(List.fill(5)(BatchMode.Sequential))
            }
          }
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
          .valueBatch
        result.assertThrowsError[Exception](_.getMessage.shouldBe("abc"))
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }

    "predicate".in {
      val jobs =
        List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
      val se = service.eventStreamR { agent =>
        val result: Resource[IO, ValueBatch[Int]] =
          agent
            .batch("predicate")
            .sequential(jobs*)
            .withPostCondition(_ > 3)
            .valueBatch
        result.assertThrowsError[PostConditionUnsatisfied](_.job.map(_.index).shouldBe(Some(1)))
      }.compile.lastOrError
      se.asserting(_.asInstanceOf[ServiceStop].cause.exitCode.shouldBe(0))
    }
  }
}
