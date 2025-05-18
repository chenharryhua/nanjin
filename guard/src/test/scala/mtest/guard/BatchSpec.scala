package mtest.guard

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.action.{Batch, MeasuredBatch, HandleJobLifecycle}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.Json
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class BatchSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("batch").updateConfig(_.withMetricReport(Policy.crontab(_.secondly)))

  private val handler = HandleJobLifecycle[IO, Json]
    .onError((job, _) => IO.println(job))
    .onCancel(IO.println)
    .onComplete((job, _) => IO.println(job))
    .onKickoff(IO.println)

  "monadic" -
    "filter - quasi".in {
      val se = service.eventStream { agent =>
        val result: IO[MeasuredBatch] = agent
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
          .quasiTrace(handler)
          .memoizedAcquire
          .use(identity)
        result.asserting { qr =>
          qr.jobs.size.shouldBe(2)
          qr.jobs(1).done.shouldBe(false)
          qr.jobs.head.done.shouldBe(true)
        }.void
      }.debug().compile.lastOrError.unsafeRunSync()

      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }

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
        .fullyTrace(handler)
        .map(_.value)
        .memoizedAcquire
        .use(identity)
      result.assertThrowsError[Batch.PostConditionUnsatisfied](_.job.name.shouldBe("b")).void
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  "invincible".in {
    val se = service.eventStream { agent =>
      val result  = agent
        .batch("monadic")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            _ <- job.invincible("b", IO.raiseError[Boolean](new Exception()))(identity)
            c <- job("c", IO(2))
          } yield a + c
        }
        .fullyTrace(handler)
        .use(qr => agent.adhoc.report.as(qr))

      result.asserting(_.value.shouldBe(3)) >>
        result.asserting(_.batch.jobs.head.done.shouldBe(true)) >>
        result.asserting(_.batch.jobs(1).done.shouldBe(false)) >>
        result.asserting(_.batch.jobs(2).done.shouldBe(true)) >>
        IO.unit
    }.evalTap(console.text[IO]).compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }
}
