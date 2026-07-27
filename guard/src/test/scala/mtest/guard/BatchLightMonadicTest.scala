package mtest.guard

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{BatchKind, BatchMode, PostConditionUnsatisfied}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.syntax.given
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

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
              a <- job("a", IO(1))
              b <- job("b", IO(2))
              c <- job("c", IO(3))
            } yield a + b + c
          }
          .withJobRename("renamed-" + _)
          .batchValue
          .map { monadicValue =>
            println(monadicValue.asJson)
            monadicValue.value shouldBe 6
            monadicValue.state.jobs.size shouldBe 3
            monadicValue.state.jobs.forall(_.job.name.startsWith("renamed-")) shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "exception" in {
      var aExecuted = false
      var bExecuted = false
      var cExecuted = false

      val se = service.eventStreamR { agent =>
        Resource.eval(
          agent
            .batchLight("light-exception")
            .monadic { job =>
              for {
                a <- job("a", IO { aExecuted = true; 1 })
                b <- job("b", IO { bExecuted = true } *> IO.raiseError[Int](new Exception("boom")))
                c <- job("c", IO { cExecuted = true; 3 })
              } yield a + b + c
            }
            .batchValue
            .attempt
            .map { outcome =>
              outcome.isLeft shouldBe true
              aExecuted shouldBe true
              bExecuted shouldBe true
              cExecuted shouldBe false
            }
        )
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
          .batchValue
          .map { monadicValue =>
            monadicValue.value shouldBe 4
            monadicValue.state.jobs.size shouldBe 3
            monadicValue.state.jobs.head.done shouldBe true
            monadicValue.state.jobs(1).done shouldBe false
            monadicValue.state.jobs(2).done shouldBe true
            aExecuted shouldBe true
            bExecuted shouldBe true
            cExecuted shouldBe true
            ()
          }
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "withFilter should fail when predicate is not satisfied" in {
      val se = service.eventStreamR { agent =>
        Resource.eval(
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
            .batchValue
            .attempt
            .map { outcome =>
              outcome.fold(_.isInstanceOf[PostConditionUnsatisfied], _ => false) shouldBe true
            }
        )
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }

    "tuple overload should work for apply and failSafe" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-tuple")
          .monadic { job =>
            for {
              a <- job("a" -> IO(1))
              b <- job.failSafe("b" -> IO.raiseError[Boolean](new Exception("boom")))
              c <- job("c" -> IO(3))
            } yield if (b) a + c + 100 else a + c
          }
          .batchValue
          .map { monadicValue =>
            monadicValue.value shouldBe 4
            monadicValue.state.jobs.size shouldBe 3
            monadicValue.state.jobs(1).job.kind shouldBe BatchKind.Quasi
            monadicValue.state.jobs(1).done shouldBe false
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
          .withJobRename("seq-" + _)
          .withPredicate(_ >= 2)
          .quasiBatch
          .map { state =>
            state.jobs.size shouldBe 3
            state.jobs.head.job.name shouldBe "seq-a"
            state.jobs.head.job.mode shouldBe BatchMode.Sequential
            state.jobs.head.job.kind shouldBe BatchKind.Quasi
            state.jobs.head.done shouldBe false
            state.jobs(1).done shouldBe true
            state.jobs(2).done shouldBe true
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
            .withPredicate(_ > 1)
            .batchValue
            .attempt
            .map { outcome =>
              outcome.fold(_.isInstanceOf[PostConditionUnsatisfied], _ => false) shouldBe true
            }
        )
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }
  }

  "parallel" - {
    "parallel(fas*) should create parallel mode using input size" in {
      val se = service.eventStream { agent =>
        agent
          .batchLight("light-parallel-default")
          .parallel("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
          .withJobRename("par-" + _)
          .withPredicate(_ >= 2)
          .quasiBatch
          .map { state =>
            state.jobs.size shouldBe 3
            state.jobs.head.job.name shouldBe "par-a"
            state.jobs.head.job.mode shouldBe BatchMode.Parallel(3)
            state.jobs.head.job.kind shouldBe BatchKind.Quasi
            state.jobs.head.done shouldBe false
            state.jobs(1).done shouldBe true
            state.jobs(2).done shouldBe true
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
            value.jobs.map(_.value).sum shouldBe 3
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
            .withPredicate(_ > 1)
            .batchValue
            .attempt
            .map { outcome =>
              outcome.fold(_.isInstanceOf[PostConditionUnsatisfied], _ => false) shouldBe true
            }
        )
      }.compile.lastOrError.unsafeRunSync()

      se.asInstanceOf[ServiceStop].cause.exitCode shouldBe 0
    }
  }
}
