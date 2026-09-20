package mtest.guard

import cats.effect.IO
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{BatchKind, BatchMode, PostConditionUnsatisfied, ValueBatch}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import munit.CatsEffectSuite

class BatchSequentialSpec extends CatsEffectSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("sequential")

  test("quasi: good job") {
    val jobs = List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
    service.eventStreamR { agent =>
      agent.batch("good job").sequential(jobs*).quasiBatch
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("quasi: exception") {
    val jobs =
      List("a" -> IO(1), "b" -> IO.raiseError(new Exception()), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
    service.eventStreamR { agent =>
      agent.batch("exception").sequential(jobs*).quasiBatch.evalTap { mb =>
        IO {
          assert(mb.outcomes.head.record.succeeded)
          assertEquals(mb.outcomes.head.record.job.mode, BatchMode.Sequential)
          assertEquals(mb.outcomes.head.record.job.kind, Option(BatchKind.Quasi))
          assert(!mb.outcomes(1).record.succeeded)
          assert(mb.outcomes(2).record.succeeded)
          assert(mb.outcomes(3).record.succeeded)
          assert(mb.outcomes(4).record.succeeded)
        }
      }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("quasi: predicate") {
    val jobs =
      List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
    service.eventStreamR { agent =>
      agent.batch("predicate").sequential(jobs*).withPostCondition(_ > 3).quasiBatch.evalTap { mb =>
        IO {
          assert(!mb.outcomes.head.record.succeeded)
          assert(!mb.outcomes(1).record.succeeded)
          assert(!mb.outcomes(2).record.succeeded)
          assert(mb.outcomes(3).record.succeeded)
          assert(mb.outcomes(4).record.succeeded)
        }
      }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("value: good job") {
    val jobs = List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
    service.eventStreamR { agent =>
      agent
        .batch("good job")
        .sequential(jobs*)
        .valueBatch
        .evalTap { bv =>
          IO {
            assertEquals(bv.outcomes.map(_.record.job.kind), List.fill(5)(Option(BatchKind.Value)))
            assertEquals(bv.outcomes.map(_.record.job.mode), List.fill(5)(BatchMode.Sequential))
          }
        }
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("value: exception") {
    val jobs =
      List("a" -> IO(1), "b" -> IO.raiseError(new Exception("abc")), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
    service.eventStreamR { agent =>
      Resource.eval(
        agent
          .batch("exception")
          .sequential(jobs*)
          .valueBatch
          .use_
          .attempt
          .map {
            case Left(e: Exception) => assertEquals(e.getMessage, "abc")
            case other              => fail(s"expected Exception(abc), got $other")
          })
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("value: predicate") {
    val jobs =
      List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3), "d" -> IO(4), "e" -> IO(5))
    service.eventStreamR { agent =>
      val result: Resource[IO, ValueBatch[Int]] =
        agent
          .batch("predicate")
          .sequential(jobs*)
          .withPostCondition(_ > 3)
          .valueBatch
      Resource.eval(result.use_.attempt.map {
        case Left(e: PostConditionUnsatisfied) => assertEquals(e.job.map(_.index), Some(1))
        case other                             => fail(s"expected PostConditionUnsatisfied, got $other")
      })
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }
}
