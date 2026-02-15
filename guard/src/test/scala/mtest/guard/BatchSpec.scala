package mtest.guard

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{
  JobHandler,
  JobResultState,
  PostConditionUnsatisfied,
  TraceJob
}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.Json
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class BatchSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("batch").updateConfig(_.withMetricReport(_.crontab(_.secondly)))

  "monadic" -
    "filter - fully".in {
      val se = service.eventStream { agent =>
        val result: IO[Int] = agent
          .batch("monadic")
          .monadic { job =>
            for {
              a <- job("a", IO(1))
              if a == 1
              b <- job("b", IO(2))
              if a == 10
              c <- job("c", IO(3))
            } yield a + b + c
          }
          .batchValue(TraceJob(agent).standard)
          .map(_.value)
          .memoizedAcquire
          .use(identity)
        result.assertThrowsError[PostConditionUnsatisfied](_.job.name.shouldBe("b")).void
      }.compile.lastOrError.unsafeRunSync()

      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }

  "invincible".in {
    val se = service.eventStream { agent =>
      val result = agent
        .batch("monadic")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            _ <- job.failSafe("b", IO.raiseError[Int](new Exception()))(new JobHandler[Int] {
              override def predicate(a: Int): Boolean = true
              override def translate(a: Int, jrs: JobResultState): Json = Json.Null
            })
            c <- job("c", IO(2))
          } yield a + c
        }
        .batchValue(TraceJob(agent).standard)
        .use(qr => agent.adhoc.report.as(qr))

      result.asserting(_.value.shouldBe(3)) >>
        result.asserting(_.resultState.jobs.head.done.shouldBe(true)) >>
        result.asserting(_.resultState.jobs(1).done.shouldBe(false)) >>
        result.asserting(_.resultState.jobs(2).done.shouldBe(true)) >>
        IO.unit
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }
}
