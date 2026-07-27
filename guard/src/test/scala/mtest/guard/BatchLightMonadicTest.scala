package mtest.guard

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.JobHandler
import com.github.chenharryhua.nanjin.guard.batch.JobState
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.Json
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
            monadicValue.state.jobs.forall(_.job.name.startsWith("renamed-"))
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
              b <- job.failSafe("b", IO { bExecuted = true } *> IO.raiseError[Int](new Exception("boom")))(
                new JobHandler[Int] {
                  override def predicate(a: Int): Boolean = true
                  override def translate(a: Int, jrs: JobState): Json = Json.fromInt(a)
                })
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
  }
}
