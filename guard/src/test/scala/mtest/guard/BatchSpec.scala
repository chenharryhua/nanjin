package mtest.guard

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.action.{Batch, QuasiResult}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class BatchSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("batch").updateConfig(_.withMetricReport(Policy.crontab(_.secondly)))

  "monadic" -
    "filter - quasi".in {
      val se = service.eventStream { agent =>
        val result: IO[QuasiResult] = agent
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
          .runQuasi
          .memoizedAcquire
          .use(identity)
        result.asserting { qr =>
          qr.details.size.shouldBe(2)
          qr.details(1).done.shouldBe(false)
          qr.details.head.done.shouldBe(true)
        }.void
      }.compile.lastOrError.unsafeRunSync()

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
        .runFully
        .memoizedAcquire
        .use(identity)
      result.assertThrowsError[Batch.PostConditionUnsatisfied](_.job.name.get.shouldBe("b")).void
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }
}
