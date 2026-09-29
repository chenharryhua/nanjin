package mtest.guard

import cats.effect.{IO, Resource}
import cats.implicits.catsSyntaxApplicativeId
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{BatchMode, PostConditionUnsatisfied}
import com.github.chenharryhua.nanjin.guard.event.Event.{ReportedEvent, ServiceStop}
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.CatsEffectSuite

import scala.concurrent.duration.DurationInt

class BatchMonadicTest extends CatsEffectSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("monadic")
    // .updateConfig(_.withLogFormat(_.ConsolePlainText).withLogThreshold(_.Info, _.Info))

  test("1.good") {
    service.eventStreamR { agent =>
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
        .monadicBatch
        .evalTap { mb =>
          IO {
            // pure steps create no job entry; only the three plain apply jobs are recorded
            assert(mb.outcomes.map(_.record.job.name) == List("a", "b", "c"))
            // monadic jobs have no kind
            assert(mb.outcomes.map(_.record.job.kind) == List.fill(3)(None))
            assert(mb.outcomes.map(_.record.job.mode) == List.fill(3)(BatchMode.Monadic))
          }
        }
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("2.exception") {
    service.eventStreamR { agent =>
      agent
        .batch("exception")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            b <- job("b", IO.raiseError[Int](new Exception()))
            c <- job("c", IO(3))
          } yield a + b + c
        }
        .monadicBatch
        .map { monadicValue =>
          assert(monadicValue.result.isLeft)
          assert(monadicValue.result.left.toOption.get.isInstanceOf[Exception])
          // the failing job (index 2) is recorded and marked unsuccessful
          val failed = monadicValue.outcomes.find(_.record.job.index == 2).get
          assert(!failed.succeeded)
        }
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("3.exception aborts the monadic chain") {
    var cExecuted = false
    service.eventStreamR { agent =>
      agent
        .batch("invincible")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            _ <- job("b", IO.raiseError[Int](new Exception()))
            c <- job("c", IO { cExecuted = true; 3 })
          } yield a + c
        }
        .monadicBatch
        .evalTap { mb =>
          IO {
            // the exception short-circuits: c never runs and is not recorded
            assert(mb.result.isLeft)
            val sorted = mb.outcomes.sortBy(_.record.job.index)
            assert(sorted.size == 2)

            assert(sorted.head.succeeded)
            assert(sorted.head.record.job.index == 1)

            assert(!sorted(1).succeeded)
            assert(sorted(1).record.job.index == 2)

            assert(!cExecuted)
          }
        }
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("4.withFilter rejects and aborts the chain") {
    service.eventStreamR { agent =>
      agent
        .batch("invincible")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            _ <- job("b", IO(2)).withFilter(_ => false)
            c <- job("c", IO(3))
          } yield a + c
        }
        .monadicBatch
        .evalTap { mb =>
          IO {
            val sorted = mb.outcomes.sortBy(_.record.job.index)

            assertEquals(mb.result.isLeft, true)
            assert(sorted.head.succeeded)
            assert(sorted.head.record.job.index == 1)
            assert(sorted.size == 2)
            assert(sorted(1).record.job.index == 2)
            assert(sorted(1).succeeded)
          }
        }
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("4a.withFilter on a pure value should fail without crashing") {
    service.eventStreamR { agent =>
      agent
        .batch("filter-pure")
        .monadic { job =>
          job.pure(1).withFilter(_ => false)
        }
        .monadicBatch
        .map { monadicValue =>
          assert(monadicValue.result.isLeft)
          assert(monadicValue.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied])
        }
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("4b.monadic jobs are successful when their effects succeed") {
    service.eventStreamR { agent =>
      agent
        .batch("invincible-json")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            ok <- job("b", IO(2))
            ko <- job("c", IO(3))
            d <- job("d", IO(4))
          } yield a + d + ok + ko
        }
        .monadicBatch
        .evalTap { mb =>
          IO {
            val sorted = mb.outcomes.sortBy(_.record.job.index)

            assert(sorted.size == 4)
            assert(sorted.forall(_.record.job.kind.isEmpty))
            assert(sorted.head.succeeded)
            assert(sorted(1).succeeded)
            assert(sorted(2).succeeded)
            assert(sorted(3).succeeded)
          }
        }
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("4c.a thrown exception is recorded unsuccessful and aborts the chain") {
    val errorMessage = "boom"
    var cExecuted = false
    service.eventStreamR { agent =>
      agent
        .batch("fail-safe-exception")
        .monadic { job =>
          for {
            _ <- job("a", IO(1))
            _ <- job("b", IO.raiseError[Int](new Exception(errorMessage)))
            _ <- job("c", IO { cExecuted = true; 3 })
          } yield ()
        }
        .monadicBatch
        .evalTap { mb =>
          IO {
            assert(mb.result.isLeft)
            val sorted = mb.outcomes.sortBy(_.record.job.index)
            // c never runs; only a and b are recorded
            assert(sorted.size == 2)
            assert(sorted.forall(_.record.job.kind.isEmpty))
            assert(!sorted(1).succeeded)
            assert(!cExecuted)
          }
        }
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("5.filter") {
    service.eventStreamR { agent =>
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
        .monadicBatch
        .map { monadicValue =>
          assert(monadicValue.result.isLeft)
          assert(monadicValue.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied])

          val sorted = monadicValue.outcomes.sortBy(_.record.job.index)
          assert(sorted.size == 2)
          assert(sorted.head.succeeded)
          assert(sorted.head.record.job.index == 1)
          assert(sorted(1).succeeded)
          assert(sorted(1).record.job.index == 2)
        }
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("5b.filter should preserve post-condition failure in job state") {
    var cExecuted = false

    service.eventStreamR { agent =>
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
        .monadicBatch
        .map { monadicValue =>
          assert(monadicValue.result.isLeft)
          assert(monadicValue.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied])

          val sorted = monadicValue.outcomes.sortBy(_.record.job.index)
          assert(sorted.size == 2)
          assert(sorted.head.succeeded)
          assert(sorted(1).succeeded)
          assert(sorted(1).record.job.index == 2)
        }
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
      assert(!cExecuted)
    }
  }

  test("6.cancel") {
    service.eventStream { agent =>
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
        .monadicBatch
        .memoizedAcquire
        .use(_.timeout(3.second))
        .attempt
        .void
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("shared MonadicOps compose map, flatMap, attempt, and withFilter") {
    service.eventStreamR { agent =>
      Resource.eval(
        agent
          .batch("shared-monadic-ops")
          .monadic { job =>
            for {
              start <- job("start", IO.pure(1)).map(_ + 1)
              captured <- job("failure", IO.raiseError[Int](new Exception("handled"))).attempt
              inside <- job("inside", IO.raiseError[Int](new Exception("handled inside")).attempt)
              double <- job("double", IO.raiseError[Int](new Exception("handled double")).attempt).attempt
              result <- job("finish", IO.pure(start + captured.fold(_ => 2, identity))).withFilter(_ == 4)
            } yield {
              assert(captured.isLeft)
              assert(inside.isLeft)
              assert(double.flatten.isLeft)
              result
            }
          }
          .monadicBatch
          .use { batch =>
            IO {
              assertEquals(batch.result, Right(4))
              assertEquals(
                batch.outcomes.map(_.record.job.name),
                List("start", "failure", "inside", "double", "finish"))
              assert(batch.outcomes.head.succeeded)
              assert(!batch.outcomes(1).succeeded)
              assert(batch.outcomes(2).succeeded)
              assert(batch.outcomes(3).succeeded)
              assert(batch.outcomes(4).succeeded)
            }
          })
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic predicate records tracked outcomes and attempt preserves rejection") {
    service.eventStream { agent =>
      for {
        batch <- agent
          .batch("monadic-predicate")
          .monadic { job =>
            for {
              rejected <- job("rejected", IO.pure(1)).predicate(_ < 0).attempt
              accepted <- job("accepted", IO.pure(2)).predicate(_ == 2)
              untracked <- job.untracked(IO.pure(3)).predicate(_ => false)
            } yield (rejected, accepted, untracked)
          }
          .monadicBatch
          .use(result => IO.pure(result))
        light <- agent
          .batchLight("light-monadic-predicate")
          .monadic { job =>
            for {
              rejected <- job("rejected", IO.pure(1)).predicate(_ < 0).attempt
              accepted <- job("accepted", IO.pure(2)).predicate(_ == 2)
              untracked <- job.untracked(IO.pure(3)).predicate(_ => false)
            } yield (rejected, accepted, untracked)
          }
          .monadicBatch
        traced <- agent
          .batchTraced("traced-monadic-predicate", _.build)
          .monadic { job =>
            for {
              rejected <- job("rejected", IO.pure(1)).predicate(_ < 0).attempt
              accepted <- job("accepted", IO.pure(2)).predicate(_ == 2)
              untracked <- job.untracked(IO.pure(3)).predicate(_ => false)
            } yield (rejected, accepted, untracked)
          }
          .monadicBatch
        _ <- IO {
          List(batch, light, traced).foreach { result =>
            assertEquals(result.result, Right((Right(1), 2, 3)))
            assertEquals(result.outcomes.map(_.record.job.name), List("rejected", "accepted"))
            assertEquals(result.outcomes.map(_.succeeded), List(false, true))
          }
        }
      } yield ()
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("monadic renderOutcome records explicit and encoded JSON outcomes") {
    val explicitJson = Json.obj("value" -> Json.fromInt(4))
    service.eventStream { agent =>
      for {
        batch <- agent
          .batch("monadic-render-outcome")
          .monadic(job =>
            job("explicit", IO.pure(4)).renderOutcome(value => Json.obj("value" -> Json.fromInt(value))))
          .monadicBatch
          .use(result => IO.pure(result))
        light <- agent
          .batchLight("light-monadic-render-outcome")
          .monadic(job => job("encoded", IO.pure(5)).render)
          .monadicBatch
        traced <- agent
          .batchTraced("traced-monadic-render-outcome", _.build)
          .monadic(job => job("encoded", IO.pure(6)).render)
          .monadicBatch
        _ <- IO {
          assertEquals(batch.result, Right(4))
          assertEquals(batch.outcomes.map(_.result), List(Right(explicitJson)))
          assertEquals(
            batch.asJson.hcursor.downField("jobs").downArray.downField("succeeded").focus,
            Some(explicitJson))

          assertEquals(light.result, Right(5))
          assertEquals(light.outcomes.map(_.result), List(Right(Json.fromInt(5))))

          assertEquals(traced.result, Right(6))
          assertEquals(traced.outcomes.map(_.result), List(Right(Json.fromInt(6))))
        }
      } yield ()
    }.compile.lastOrError.map { se =>
      assertEquals(se.asInstanceOf[ServiceStop].cause.exitCode, 0)
    }
  }

  test("attempt logs the finalized handled state on the next transition") {
    service
      .eventStream { agent =>
        agent
          .batch("attempt-lifecycle")
          .monadic { job =>
            for {
              _ <- job("failed", IO.raiseError[Int](new Exception("handled"))).attempt
              _ <- job("next", IO.pure(2))
            } yield ()
          }
          .monadicBatch
          .use { batch =>
            IO {
              assertEquals(batch.outcomes.map(_.record.job.name), List("failed", "next"))
              assert(batch.outcomes.forall(_.succeeded))
            }
          }
      }
      .collect { case event: ReportedEvent => event }
      .compile
      .toList
      .map { events =>
        val nonfatalJobs = events.flatMap { event =>
          event.logRecord.message.value.hcursor
            .downField("nonfatal")
            .downField("job-1")
            .as[String]
            .toOption
        }
        assertEquals(nonfatalJobs, List("failed"))
      }
  }
}
