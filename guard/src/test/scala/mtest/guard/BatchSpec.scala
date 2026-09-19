package mtest.guard

import cats.Applicative
import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.PostConditionUnsatisfied
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import munit.CatsEffectSuite

class BatchSpec extends CatsEffectSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("batch").updateConfig(_.withReportPolicy(_.crontab(_.secondly).repeat))

  test("monadic: filter - fully") {
    service.eventStream { agent =>
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
        .monadicBatch
        .use { monadicResult =>
          monadicResult.result match {
            case Left(ex) => IO.raiseError[Int](ex)
            case Right(v) => IO.pure(v)
          }
        }
      result.attempt.map {
        case Left(e: PostConditionUnsatisfied) => assertEquals(e.job.map(_.name), Some("b"))
        case other                             => fail(s"expected PostConditionUnsatisfied, got $other")
      }.void
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("applicative: combines values sequentially") {
    service.eventStream { agent =>
      val result = agent
        .batch("monadic")
        .monadic { job =>
          type M[A] = job.Monadic[A]
          val combined = Applicative[M].map2(job("a", IO(1)), job("b", IO(2)))(_ + _)
          combined
        }
        .monadicBatch
        .use(qr => agent.adhoc.report.as(qr))

      result.map { r =>
        assertEquals(r.result, Right(3))
        assertEquals(r.outcomes.size, 2)
      }
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("identity law preserves the job result") {
    service.eventStream { agent =>
      val left = agent
        .batch("monadic")
        .monadic { job =>
          Applicative[job.Monadic].ap(Applicative[job.Monadic].pure((x: Int) => x))(job("a", IO(1)))
        }
        .monadicBatch
        .use(_.result match {
          case Right(value) => IO.pure(value)
          case Left(ex)     => IO.raiseError(ex)
        })

      val right = agent
        .batch("monadic")
        .monadic { job =>
          job("a", IO(1))
        }
        .monadicBatch
        .use(_.result match {
          case Right(value) => IO.pure(value)
          case Left(ex)     => IO.raiseError(ex)
        })

      for {
        l <- left
        r <- right
      } yield {
        assertEquals(l, 1)
        assertEquals(r, 1)
        ()
      }
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }

  test("invincible") {
    service.eventStream { agent =>
      val result = agent
        .batch("monadic")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            _ <- job("b", IO(0), _ => false)
            c <- job("c", IO(2))
          } yield a + c
        }
        .monadicBatch
        .use(qr => agent.adhoc.report.as(qr))

      result.map { r =>
        assertEquals(r.result, Right(3))
        assert(r.outcomes.head.record.succeeded)
        assert(!r.outcomes(1).record.succeeded)
        assert(r.outcomes(2).record.succeeded)
      }
    }.compile.lastOrError.map { se =>
      assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
    }
  }
}
