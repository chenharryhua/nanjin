package mtest.guard

import cats.Applicative
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits.{catsSyntaxApplicativeId, toTraverseOps}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{BatchKind, BatchMode, PostConditionUnsatisfied}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

class BatchLightMonadicTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("batch-light")

  "monadic" - {
    "smoke" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light")
          .monadic { job =>
            for {
              _ <- job.pure(1)
              a <- job("a", IO(1))
              _ <- 2.pure[job.Monadic]
              b <- job("b", IO(2))
              _ <- job.pure(3)
              c <- job("c", IO(3))
              _ <- List(1, 2, 3).traverse(job.pure)
            } yield a + b + c
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.result shouldBe Right(6)
            monadicValue.jobs.size shouldBe 3
            monadicValue.jobs.map(_.job.name) shouldBe List("a", "b", "c")
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "completed jobs are ordered by index" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-ordered")
          .monadic { job =>
            for {
              a <- job("a", IO(1))
              b <- job("b", IO(2))
              c <- job("c", IO(3))
            } yield a + b + c
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.jobs.map(_.job.index) shouldBe List(1, 2, 3)
            monadicValue.jobs.map(_.job.name) shouldBe List("a", "b", "c")
            monadicValue.jobs.map(_.succeeded) shouldBe List(true, true, true)
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "timing is preserved in the batch report" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-timing")
          .monadic { job =>
            for {
              a <- job("a", IO.sleep(50.millis).as(1))
              b <- job("b", IO.sleep(50.millis).as(2))
            } yield a + b
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.spent.toMillis should be > 0L
            monadicValue.jobs.map(_.took.toMillis).forall(_ > 0L) shouldBe true
            monadicValue.jobs.map(_.job.name) shouldBe List("a", "b")
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "exception" in {
      var aExecuted = false
      var bExecuted = false
      var cExecuted = false

      val se = service.eventStream { agent =>
        agent
          .batchLight("light-exception")
          .monadic { job =>
            for {
              a <- job("a", IO { aExecuted = true; 1 })
              b <- job("b", IO { bExecuted = true } *> IO.raiseError[Int](new Exception("boom")))
              c <- job("c", IO { cExecuted = true; 3 })
            } yield a + b + c
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.result.isLeft shouldBe true
            aExecuted shouldBe true
            bExecuted shouldBe true
            cExecuted shouldBe false
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "mix normal and exception" in {
      var aExecuted = false
      var bExecuted = false
      var cExecuted = false

      val se = service.eventStream { agent =>
        agent
          .batchLight("light-mix")
          .monadic { job =>
            for {
              a <- job("a", IO { aExecuted = true; 1 })
              b <- job.failSafe("b", IO { bExecuted = true } *> IO.raiseError[Boolean](new Exception("boom")))
              c <- job("c", IO { cExecuted = true; 3 })
            } yield if (b) a + c else a + c
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.result shouldBe Right(4)
            monadicValue.jobs.size shouldBe 3
            monadicValue.jobs.head.succeeded.shouldBe(true)
            monadicValue.jobs(1).succeeded.shouldBe(false)
            monadicValue.jobs(2).succeeded.shouldBe(true)
            aExecuted shouldBe true
            bExecuted shouldBe true
            cExecuted shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "supports applicative-style composition" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-applicative")
          .monadic { job =>
            type M[A] = job.Monadic[A]
            val combined = Applicative[M].map2(job("a", IO(1)), job("b", IO(2)))(_ + _)
            combined
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.result shouldBe Right(3)
            monadicValue.jobs.size shouldBe 2
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "identity law preserves the job result" in {
      val se = service.eventStream { agent =>
        val left = agent
          .batchLight("light-applicative-identity")
          .monadic { job =>
            Applicative[job.Monadic].ap(Applicative[job.Monadic].pure((x: Int) => x))(job("a", IO(1)))
          }
          .monadicBatch
          .map(_.result)

        val right = agent
          .batchLight("light-applicative-identity-right")
          .monadic { job =>
            job("a", IO(1))
          }
          .monadicBatch
          .map(_.result)

        val combined = for {
          l <- left
          r <- right
        } yield {
          l shouldBe Right(1)
          r shouldBe Right(1)
          ()
        }

        combined
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "withFilter on a pure value should fail without crashing (light-1)" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-filter-pure")
          .monadic { job =>
            job.pure(1).withFilter(_ => false)
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.result.isLeft shouldBe true
            monadicValue.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied] shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "withFilter should fail when predicate is not satisfied" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-filter")
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
            monadicValue.result.isLeft shouldBe true
            monadicValue.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied] shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "withFilter on a pure value should fail without crashing (light-2)" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-filter-pure")
          .monadic { job =>
            job.pure(1).withFilter(_ => false)
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.result.isLeft shouldBe true
            monadicValue.result.left.toOption.get.isInstanceOf[PostConditionUnsatisfied] shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "failSafe true should mark quasi job done" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-tuple")
          .monadic { job =>
            for {
              a <- job("a", IO(1))
              b <- job.failSafe("b", IO(true))
              c <- job("c", IO(3))
            } yield if (b) a + c + 100 else a + c
          }
          .monadicBatch
          .map { monadicValue =>
            monadicValue.result shouldBe Right(104)
            monadicValue.jobs.size shouldBe 3
            monadicValue.jobs(1).job.kind shouldBe BatchKind.Quasi
            monadicValue.jobs(1).succeeded.shouldBe(true)
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }
  }

  "sequential" - {
    "quasiBatch should support rename and predicate" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-sequential")
          .sequential("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
          .withPostCondition(_ >= 2)
          .quasiBatch
          .map { state =>
            state.jobs.size shouldBe 3
            state.jobs.head.completed.job.name shouldBe "a"
            state.jobs.head.completed.job.mode shouldBe BatchMode.Sequential
            state.jobs.head.completed.job.kind shouldBe BatchKind.Quasi
            state.jobs.head.completed.succeeded shouldBe false
            state.jobs(1).completed.succeeded shouldBe true
            state.jobs(2).completed.succeeded shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "completed jobs are ordered by index" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-sequential-ordered")
          .sequential("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
          .quasiBatch
          .map { state =>
            state.jobs.map(_.completed.job.index) shouldBe List(1, 2, 3)
            state.jobs.map(_.completed.job.name) shouldBe List("a", "b", "c")
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "batchValue should fail when predicate is not satisfied" in {
      val se = service.eventStreamR { agent =>
        Resource.eval(
          agent
            .batchLight("light-sequential-value")
            .sequential("a" -> IO(1), "b" -> IO(2))
            .withPostCondition(_ > 1)
            .batchValue
            .attempt
            .map { outcome =>
              outcome.fold(_.isInstanceOf[PostConditionUnsatisfied], _ => false) shouldBe true
            }
        )
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "batchValue should return all values on success" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-sequential-value-ok")
          .sequential("a" -> IO(10), "b" -> IO(20), "c" -> IO(30))
          .batchValue
          .map { bv =>
            bv.jobs.size shouldBe 3
            bv.jobs.map(_.result) shouldBe List(10, 20, 30)
            bv.mode shouldBe BatchMode.Sequential
            bv.succeeded shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }
  }

  "parallel" - {
    "parallel(0) should fail fast" in {
      val se = service.eventStreamR { agent =>
        Resource.eval(
          agent
            .batchLight("light-parallel-invalid")
            .parallel(0)("a" -> IO(1))
            .quasiBatch
            .attempt
            .map { outcome =>
              outcome.fold(_.getMessage.contains("parallelism must be > 0"), _ => false) shouldBe true
            }
        )
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 3
    }

    "parallel(fas*) should fail fast when empty" in {
      val se = service.eventStreamR { agent =>
        Resource.eval(
          agent
            .batchLight("light-parallel-empty")
            .parallel[Int]()
            .quasiBatch
            .attempt
            .map { outcome =>
              outcome.fold(_.getMessage.contains("parallelism must be > 0"), _ => false) shouldBe true
            }
        )
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 3
    }

    "parallel(fas*) should create parallel mode using input size" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-parallel-default")
          .parallel("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
          .withPostCondition(_ >= 2)
          .quasiBatch
          .map { state =>
            state.jobs.size shouldBe 3
            state.jobs.head.completed.job.name shouldBe "a"
            state.jobs.head.completed.job.mode shouldBe BatchMode.Parallel(3)
            state.jobs.head.completed.job.kind shouldBe BatchKind.Quasi
            state.jobs.head.completed.succeeded shouldBe false
            state.jobs(1).completed.succeeded shouldBe true
            state.jobs(2).completed.succeeded shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "parallel(parallelism)(fas*) should use explicit parallelism" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-parallel-explicit")
          .parallel(1)("a" -> IO(1), "b" -> IO(2))
          .batchValue
          .map { value =>
            value.jobs.size shouldBe 2
            value.mode shouldBe BatchMode.Parallel(1)
            value.jobs.map(_.result).sum shouldBe 3
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "completed jobs are ordered by index" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-parallel-ordered")
          .parallel(3)("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
          .quasiBatch
          .map { state =>
            state.jobs.map(_.completed.job.index) shouldBe List(1, 2, 3)
            state.jobs.map(_.completed.job.name) shouldBe List("a", "b", "c")
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "batchValue should fail when predicate is not satisfied" in {
      val se = service.eventStreamR { agent =>
        Resource.eval(
          agent
            .batchLight("light-parallel-value")
            .parallel(2)("a" -> IO(1), "b" -> IO(2))
            .withPostCondition(_ > 1)
            .batchValue
            .attempt
            .map { outcome =>
              outcome.fold(_.isInstanceOf[PostConditionUnsatisfied], _ => false) shouldBe true
            }
        )
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "failed action cancels sibling jobs" in {
      var aCompleted = false
      var cCompleted = false

      val se = service.eventStreamR { agent =>
        Resource.eval(
          agent
            .batchLight("light-failed-cancels-siblings")
            .parallel(3)(
              "a" -> IO.sleep(2.seconds) *> IO { aCompleted = true; 1 },
              "b" -> IO.raiseError[Int](new Exception("boom")),
              "c" -> IO.sleep(5.seconds) *> IO { cCompleted = true; 3 }
            )
            .batchValue
            .attempt
            .map { outcome =>
              outcome.isLeft shouldBe true
              aCompleted shouldBe false
              cCompleted shouldBe false
            }
        )
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }
  }
}
