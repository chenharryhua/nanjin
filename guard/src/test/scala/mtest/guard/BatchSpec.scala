package mtest.guard

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{JobHook, PostConditionUnsatisfied}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class BatchSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("batch").updateConfig(_.withMetricsReport(_.crontab(_.secondly)))

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
          .monadicResult(JobHook.noop)
          .use { monadicResult =>
            monadicResult.result match {
              case Left(ex)  => IO.raiseError[Int](ex)
              case Right(v) => IO.pure(v)
            }
          }
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
            _ <- job.failSafe("b", IO.raiseError[Boolean](new Exception()))
            c <- job("c", IO(2))
          } yield a + c
        }
        .monadicResult(JobHook.noop)
        .use(qr => agent.adhoc.report.as(qr))

      result.asserting(_.result.shouldBe(Right(3))) >>
        result.asserting(_.jobs.head.done.shouldBe(true)) >>
        result.asserting(_.jobs(1).done.shouldBe(false)) >>
        result.asserting(_.jobs(2).done.shouldBe(true)) >>
        IO.unit
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }
}
