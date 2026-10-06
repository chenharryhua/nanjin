package mtest.guard

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{JobFlag, PostConditionUnsatisfied}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import munit.CatsEffectSuite
import org.typelevel.otel4s.trace.Span

/** Pins the quasi and value classification rules to the same observable outcomes across the façades.
  *
  * `Batch` and `BatchMetered` classify through `JobExecutor`, while `BatchTraced` deliberately carries its
  * own copy of those rules. Nothing in the type system keeps the two copies in step, so these tests compare
  * them end to end through the public API: same jobs, same post-condition, same `(flag, result)` per job.
  * Records are dropped from the comparison because timing and span context legitimately differ.
  */
class BatchClassificationParitySpec extends CatsEffectSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("classification-parity")

  private val boom = new Exception("boom")

  /** The classification of a job, with the record dropped and the error reduced to its message. */
  private def classify(flag: JobFlag, result: Either[Throwable, Int]): (JobFlag, Either[String, Int]) =
    (flag, result.left.map(_.getMessage))

  /** One passing job, one rejected by the post-condition, one that throws. */
  private val jobs: List[(String, IO[Int])] =
    List("pass" -> IO.pure(2), "miss" -> IO.pure(1), "throw" -> IO.raiseError[Int](boom))

  private val tracedJobs: List[(String, Span[IO] => IO[Int])] =
    jobs.map { case (name, fa) => name -> ((_: Span[IO]) => fa) }

  test("1.quasi classification agrees between Batch and BatchTraced") {
    service
      .eventStream { agent =>
        for {
          untraced <- agent
            .batch("parity-quasi")
            .sequential(jobs*)
            .withPostCondition(_ > 1)
            .quasiBatch
          traced <- agent
            .batchTraced("parity-quasi-traced", _.build)
            .sequential(tracedJobs*)
            .withPostCondition(_ > 1)
            .quasiBatch
          _ <- IO {
            // a quasi batch retains every outcome: the rejected value stays a Right, the throw a Left,
            // and both are flagged Unmet so neither is fatal to the batch
            val expected =
              List((JobFlag.Accepted, Right(2)), (JobFlag.Unmet, Right(1)), (JobFlag.Unmet, Left("boom")))
            assertEquals(untraced.outcomes.map(js => classify(js.flag, js.result)), expected)
            assertEquals(traced.outcomes.map(js => classify(js.flag, js.result)), expected)
          }
        } yield ()
      }
      .compile
      .lastOrError
      // the service captures a failed assertion rather than propagating it, so the stop's exit code is
      // what actually reports it
      .map(event => assertEquals(event.asInstanceOf[ServiceStop].cause.exitCode, 0))
  }

  test("2.value classification agrees between Batch and BatchTraced") {
    service
      .eventStream { agent =>
        def untracedValue(label: String, fa: IO[Int], p: Int => Boolean) =
          agent.batch(label).sequential("job" -> fa).withPostCondition(p).valueBatch.attempt

        def tracedValue(label: String, fa: IO[Int], p: Int => Boolean) =
          agent
            .batchTraced(label, _.build)
            .sequential("job" -> ((_: Span[IO]) => fa))
            .withPostCondition(p)
            .valueBatch
            .attempt

        for {
          untracedOk <- untracedValue("parity-value-ok", IO.pure(2), _ > 1)
          tracedOk <- tracedValue("parity-value-ok-traced", IO.pure(2), _ > 1)
          untracedMiss <- untracedValue("parity-value-miss", IO.pure(1), _ > 1)
          tracedMiss <- tracedValue("parity-value-miss-traced", IO.pure(1), _ > 1)
          untracedBoom <- untracedValue("parity-value-boom", IO.raiseError[Int](boom), _ => true)
          tracedBoom <- tracedValue("parity-value-boom-traced", IO.raiseError[Int](boom), _ => true)
          _ <- IO {
            // a passing value is returned
            assertEquals(untracedOk.toOption.map(_.result), Some(List(2)))
            assertEquals(tracedOk.toOption.map(_.result), Some(List(2)))
            // a post-condition miss is folded into PostConditionUnsatisfied and raised
            assert(untracedMiss.left.toOption.exists(_.isInstanceOf[PostConditionUnsatisfied]))
            assert(tracedMiss.left.toOption.exists(_.isInstanceOf[PostConditionUnsatisfied]))
            // a thrown job is raised unchanged
            assertEquals(untracedBoom.left.toOption.map(_.getMessage), Some("boom"))
            assertEquals(tracedBoom.left.toOption.map(_.getMessage), Some("boom"))
          }
        } yield ()
      }
      .compile
      .lastOrError
      .map(event => assertEquals(event.asInstanceOf[ServiceStop].cause.exitCode, 0))
  }
}
