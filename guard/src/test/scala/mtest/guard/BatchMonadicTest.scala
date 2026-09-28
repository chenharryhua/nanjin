package mtest.guard

import cats.effect.{IO, Resource}
import cats.implicits.catsSyntaxApplicativeId
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{BatchMode, PostConditionUnsatisfied}
import com.github.chenharryhua.nanjin.guard.event.Event.{ReportedEvent, ServiceStop}
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
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
          assert(!failed.passed)
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

            assert(sorted.head.passed)
            assert(sorted.head.record.job.index == 1)

            assert(!sorted(1).passed)
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
            assert(sorted.head.passed)
            assert(sorted.head.record.job.index == 1)
            assert(sorted.size == 2)
            assert(sorted(1).record.job.index == 2)
            assert(sorted(1).passed)
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
            assert(sorted.head.passed)
            assert(sorted(1).passed)
            assert(sorted(2).passed)
            assert(sorted(3).passed)
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
            assert(!sorted(1).passed)
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
          assert(sorted.head.passed)
          assert(sorted.head.record.job.index == 1)
          assert(sorted(1).passed)
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
          assert(sorted.head.passed)
          assert(sorted(1).passed)
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
              result <- job("finish", IO.pure(start + captured.fold(_ => 2, identity))).withFilter(_ == 4)
            } yield result
          }
          .monadicBatch
          .use { batch =>
            IO {
              assertEquals(batch.result, Right(4))
              assertEquals(batch.outcomes.map(_.record.job.name), List("start", "failure", "finish"))
              assert(batch.outcomes.head.passed)
              assert(!batch.outcomes(1).passed)
              assert(batch.outcomes(2).passed)
            }
          })
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
              assert(batch.outcomes.forall(_.passed))
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
