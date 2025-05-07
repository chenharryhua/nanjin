package mtest.guard

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.action.{Batch, BatchResult, HandleJobLifecycle}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class BatchSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("batch").service("batch").updateConfig(_.withMetricReport(Policy.crontab(_.secondly)))

  private val handler = HandleJobLifecycle[IO, Unit]
    .onError((job, _) => IO.println(job))
    .onCancel(IO.println)
    .onComplete((job, _) => IO.println(job))
    .onKickoff(IO.println)

  "monadic" -
    "filter - quasi".in {
      val se = service.eventStream { agent =>
        val result: IO[BatchResult] = agent
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
          .renameJobs("monadic:" + _)
          .traceQuasi(handler)
          .memoizedAcquire
          .use(identity)
        result.asserting { qr =>
          qr.results.size.shouldBe(2)
          qr.results(1).done.shouldBe(false)
          qr.results.head.done.shouldBe(true)
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
        .traceFully(handler)
        .map(_._2)
        .memoizedAcquire
        .use(identity)
      result.assertThrowsError[Batch.PostConditionUnsatisfied](_.job.name.shouldBe("b")).void
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }
}
