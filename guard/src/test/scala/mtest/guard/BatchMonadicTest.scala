package mtest.guard

import cats.effect.IO
import cats.implicits.catsSyntaxApplicativeId
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{BatchMode, PostConditionUnsatisfied}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
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
          assert(!failed.record.succeeded)
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

            assert(sorted.head.record.succeeded)
            assert(sorted.head.record.job.index == 1)

            assert(!sorted(1).record.succeeded)
            assert(sorted(1).record.job.index == 2)

            assert(!cExecuted)
          }
        }
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("4.rejected predicate is recorded unsuccessful but does not abort") {
    service.eventStreamR { agent =>
      agent
        .batch("invincible")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            _ <- job("b", IO(2), _ => false)
            c <- job("c", IO(3))
          } yield a + c
        }
        .monadicBatch
        .evalTap { mb =>
          IO {
            val sorted = mb.outcomes.sortBy(_.record.job.index)

            assert(sorted.head.record.succeeded)
            assert(sorted.head.record.job.index == 1)

            // a rejected predicate is recorded as unsuccessful but does not abort the batch
            assert(!sorted(1).record.succeeded)
            assert(sorted(1).record.job.index == 2)

            assert(sorted(2).record.succeeded)
            assert(sorted(2).record.job.index == 3)
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

  test("4b.predicate records job success reflecting the result, all jobs Value") {
    service.eventStreamR { agent =>
      agent
        .batch("invincible-json")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            ok <- job("b", IO(2), _ > 0)
            ko <- job("c", IO(3), _ => false)
            d <- job("d", IO(4))
          } yield a + d + ok + ko
        }
        .monadicBatch
        .evalTap { mb =>
          IO {
            val sorted = mb.outcomes.sortBy(_.record.job.index)

            assert(sorted.size == 4)
            assert(sorted.forall(_.record.job.kind.isEmpty))
            assert(sorted.head.record.succeeded)
            assert(sorted(1).record.succeeded)
            assert(!sorted(2).record.succeeded)
            assert(sorted(3).record.succeeded)
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
            assert(!sorted(1).record.succeeded)
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
          assert(sorted.head.record.succeeded)
          assert(sorted.head.record.job.index == 1)
          assert(sorted(1).record.succeeded)
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
          assert(sorted.head.record.succeeded)
          assert(sorted(1).record.succeeded)
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
}
