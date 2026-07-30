package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxTuple2Semigroupal
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.*
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.Json
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.InformationConversions

import scala.concurrent.duration.{DurationDouble, DurationInt}

class BatchTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("quasi")
      .service("quasi")
      .updateConfig(_.withMetricsReport(_.crontab(_.secondly)).withLogFormat(_.Slf4j_Json_OneLine))

  test("1.quasi.sequential") {
    val se = service.eventStream { ga =>
      ga.batch("quasi.sequential")
        .sequential[Unit](
          "a" -> IO.raiseError(new Exception()),
          "bbb" -> IO.sleep(1.second),
          "cccc" -> IO.sleep(2.seconds),
          "ddd" -> IO.raiseError(new Exception()),
          "ee" -> IO.sleep(1.seconds),
          "f" -> IO.raiseError(new Exception)
        )
        .quasiBatch(
          JobHook
            .noop[IO, Unit]
            .onKickoff(_ => IO.println("kickoff"))
            .onCancel(_ => IO.println("cancel"))
            .onComplete(_ => IO.println("complete"))
        )
        .map { qr =>
          assert(!qr.jobs.head.completed.done)
          assert(qr.jobs(1).completed.done)
          assert(qr.jobs(2).completed.done)
          assert(!qr.jobs(3).completed.done)
          assert(qr.jobs(4).completed.done)
          assert(!qr.jobs(5).completed.done)
          assert(qr.jobs.map(_.completed.job.name).toList == List("a", "bbb", "cccc", "ddd", "ee", "f"))
          qr
        }
        .use(qr => IO.println(qr.asJson) <* ga.adhoc.report)
    }.map(checkJson).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("2.quasi.parallel") {
    val se = service.eventStream { ga =>
      ga.batch("quasi.parallel")
        .parallel(3)(
          "a" -> IO.sleep(3.second),
          "bb" -> IO.sleep(2.seconds),
          "cccc" -> IO.raiseError(new Exception),
          "ddd" -> IO.sleep(3.seconds),
          "ee" -> IO.raiseError(new Exception),
          "f" -> IO.sleep(4.seconds)
        )
        .quasiBatch(JobHook(ga.logger).universal[Unit](_.asJson).onKickoff(_ => IO.unit))
        .map { qr =>
          assert(qr.jobs.head.completed.done)
          assert(qr.jobs(1).completed.done)
          assert(!qr.jobs(2).completed.done)
          assert(qr.jobs(3).completed.done)
          assert(!qr.jobs(4).completed.done)
          assert(qr.jobs(5).completed.done)
          assert(qr.jobs.map(_.completed.job.name).toList == List("a", "bb", "cccc", "ddd", "ee", "f"))
          qr
        }
        .use(_ => ga.adhoc.report.void)
    }.map(checkJson).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

  }

  test("3.sequential") {
    val se = service.eventStream { agent =>
      agent
        .batch("sequential")
        .sequential(
          "a" -> IO.sleep(1.second).as(1.mb),
          "b" -> IO.sleep(2.seconds).as(2.tb),
          "c" -> IO.sleep(1.seconds).as(3.bytes))
        .batchValue(JobHook.noop)
        .use_
    }.map(checkJson).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

  }

  test("4.parallel") {
    val se = service.eventStream { ga =>
      ga.batch("parallel")
        .parallel(3)(
          "a" -> IO.sleep(3.second),
          "b" -> IO.sleep(2.seconds),
          "c" -> IO.sleep(3.seconds),
          "d" -> IO.sleep(4.seconds))
        .withPostCondition(_ => true)
        .batchValue(JobHook.noop)
        .memoizedAcquire
        .use(_.map(_.jobs.forall(_.completed.done)))
        .map(assert(_))
        .void
    }.map(checkJson).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("5.sequential.exception") {
    val se = service.eventStream { ga =>
      ga.batch("sequential")
        .sequential(
          "a" -> IO.sleep(1.second),
          "b" -> IO.sleep(2.seconds),
          "c" -> IO.raiseError(new Exception),
          "d" -> IO.sleep(1.seconds))
        .batchValue(JobHook.noop)
        .use_
    }.map(checkJson).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 3)

  }

  test("6.parallel.exception") {
    val jobs = List(
      "a" -> IO.sleep(1.second),
      "b" -> IO.sleep(2.seconds),
      "c" -> IO.sleep(3.seconds),
      "d" -> (IO.sleep(3.seconds) >> IO.raiseError(new Exception)),
      "e" -> IO.sleep(4.seconds)
    )
    val se = service.eventStream { ga =>
      ga.batch("parallel").parallel(3)(jobs*).batchValue(JobHook.noop).use_
    }.map(checkJson).compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 3)

  }

  test("7.batch mode") {
    val j1 = service
      .eventStream(
        _.batch("parallel-1")
          .parallel("a" -> IO(true))
          .quasiBatch(JobHook.noop)
          .map(r => assert(r.mode == BatchMode.Parallel(1)))
          .use_)
      .map(checkJson)
      .compile
      .drain

    val j2 = service
      .eventStream(ga =>
        ga.batch("sequential")
          .sequential("a" -> IO(true))
          .quasiBatch(JobHook.noop)
          .map(r => assert(r.mode == BatchMode.Sequential))
          .use_)
      .map(checkJson)
      .compile
      .drain
    (j1 >> j2).unsafeRunSync()
  }

  test("8.monotonic") {
    val diff = (IO.monotonic, IO.monotonic).mapN((a, b) => b - a).unsafeRunSync()
    assert(diff.toNanos > 0)
    val res = for {
      a <- IO.monotonic
      b <- IO.monotonic
      c <- IO.monotonic
    } yield (b - a, c - b)

    println(res.unsafeRunSync())
  }

  test("9.guarantee") {
    assertThrows[Exception](IO(1).guarantee(IO.raiseError(new Exception())).unsafeRunSync())
  }

  test("10.bracket") {
    assertThrows[Exception](
      IO.bracketFull(_ => IO(1))(IO.println)((_, _) => IO.raiseError(new Exception())).unsafeRunSync())
  }

  test("11.monadic") {
    service.eventStream { agent =>
      agent.batch("monadic").monadic { job =>
        job("a", IO.println(1))
          .flatMap(_ => job("b", IO.println(2)))
          .flatMap(_ => job("c", agent.adhoc.report >> IO.sleep(1.seconds)))
          .flatMap(_ => job("d", IO.println(4)))
          .flatMap(_ => job("e", agent.adhoc.report.void))
          .flatMap(_ => job("f", IO.println(6)))
          .monadicBatch(JobHook.noop)
          .use(_ => agent.adhoc.report.void)
      }
    }.compile.drain.unsafeRunSync()
  }

  test("12.monadic for comprehension") {
    val se = service.eventStream { agent =>
      agent
        .batch("monadic")
        .monadic { job =>
          for {
            a <- job("a", IO.println("a").as(10))
            b <- job("b", IO.sleep(1.seconds) >> IO.println("b").as(20))
            _ <- job("c", agent.adhoc.report.void)
            _ <- job("d", IO.println("aaaa"))
            _ <- job("e", IO.sleep(1.seconds).flatMap(_ => IO.println("bbbb")))
            _ <- job("f", agent.adhoc.report.void)
            c <- job("g", IO.println("c").as(30))
          } yield a + b + c
        }
        .monadicBatch(JobHook.noop)
        .use { qr =>
          assert(qr.result == Right(60))
          assert(qr.jobs.map(_.job.name) == List("a", "b", "c", "d", "e", "f", "g"))
          agent.adhoc.report.void
        }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

  }

  test("13.invincible monadic error ") {
    val se = service.eventStream { agent =>
      agent
        .batch("monadic")
        .monadic { job =>
          for {
            a <- job("a", IO.println("a").as(10))
            b <- job("b", IO.sleep(1.seconds) >> IO.println("b").as(20))
            _ <- job("report-1", agent.adhoc.report.void)
            _ <- job.failSafe("exception", IO.raiseError[Boolean](new Exception("aaaa")))
            _ <- job("f", IO.println("bbbb"))
            _ <- job("report-2", agent.adhoc.report.void)
            c <- job("c", IO.println("c").as(30))
          } yield a + b + c
        }
        .monadicBatch(JobHook.noop)
        .use { qr =>
          assert(qr.jobs.head.done)
          assert(qr.jobs(1).done)
          assert(qr.jobs(2).done)
          assert(!qr.jobs(3).done)
          assert(qr.jobs(4).done)
          assert(qr.jobs(5).done)
          assert(qr.jobs(6).done)
          assert(qr.jobs.size == 7)
          agent.adhoc.report.void
        }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("14.monadic one") {
    service.eventStream { agent =>
      agent
        .batch("monadic")
        .monadic(job => job("a", IO(0)))
        .monadicBatch(JobHook.noop)
        .use(_ => agent.adhoc.report.void)
    }.compile.drain.unsafeRunSync()
  }

  test("15.monadic many") {
    val se = service.eventStream { agent =>
      agent
        .batch("monadic")
        .monadic { (job: Batch.JobBuilder[IO]) =>
          val p1 = for {
            a <- job("1", IO(1))
            b <- job("2", IO(2))
            c <- job("3", IO(3))
          } yield a + b + c
          val p2 = for {
            x <- job("10", IO(10))
            y <- job("20", IO(20))
            z <- job("30", IO(30))
          } yield x + y + z

          for {
            a <- p1
            b <- p2
          } yield a + b
        }
        .monadicBatch(JobHook.noop)
        .use { qr =>
          val details = qr.jobs.sortBy(_.job.index)
          assert(details.head.job.name === "1")
          assert(details.head.job.index === 1)
          assert(details(1).job.name === "2")
          assert(details(1).job.index === 2)
          assert(details(2).job.name === "3")
          assert(details(2).job.index === 3)
          assert(details(3).job.name === "10")
          assert(details(3).job.index === 4)
          assert(details(4).job.name === "20")
          assert(details(4).job.index === 5)
          assert(details(5).job.name === "30")
          assert(details(5).job.index === 6)
          assert(details.size == 6)
          agent.adhoc.report.void
        }
    }.compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  private val jobs: List[(String, IO[Int])] = List(
    "1" -> IO(1).delayBy(3.second),
    "2" -> IO(2).delayBy(2.second),
    "3" -> IO(3).delayBy(2.second),
    "4" -> IO(4).delayBy(1.second),
    "5" -> IO(5).delayBy(0.1.second)
  )

  test("16.sorted parallel") {
    val se = service.eventStream { agent =>
      agent.batch("sorted.parallel").parallel(jobs*).batchValue(JobHook.noop).use {
        case BatchValue(_, _, _, _, rt) =>
          val jobs = rt.sortBy(_.completed.job.index)
          IO {
            assert(jobs.head.result == 1)
            assert(jobs(1).result == 2)
            assert(jobs(2).result == 3)
            assert(jobs(3).result == 4)
            assert(jobs(4).result == 5)
            assert(jobs.forall(_.completed.done))
            assert(jobs.head.completed.job.name == "1")
            assert(jobs.head.completed.job.index == 1)
            assert(jobs(1).completed.job.name == "2")
            assert(jobs(1).completed.job.index == 2)
            assert(jobs(2).completed.job.name == "3")
            assert(jobs(2).completed.job.index == 3)
            assert(jobs(3).completed.job.name == "4")
            assert(jobs(3).completed.job.index == 4)
            assert(jobs(4).completed.job.name == "5")
            assert(jobs(4).completed.job.index == 5)
          }.void
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("17.sorted sequential") {
    val se = service.eventStream { agent =>
      agent.batch("sorted.sequential").sequential(jobs*).batchValue(JobHook.noop).use {
        case BatchValue(_, _, _, _, rt) =>
          val jobs = rt.sortBy(_.completed.job.index)
          IO {
            assert(jobs.head.result == 1)
            assert(jobs(1).result == 2)
            assert(jobs(2).result == 3)
            assert(jobs(3).result == 4)
            assert(jobs(4).result == 5)
            assert(jobs.forall(_.completed.done))
            assert(jobs.head.completed.job.name == "1")
            assert(jobs.head.completed.job.index == 1)
            assert(jobs(1).completed.job.name == "2")
            assert(jobs(1).completed.job.index == 2)
            assert(jobs(2).completed.job.name == "3")
            assert(jobs(2).completed.job.index == 3)
            assert(jobs(3).completed.job.name == "4")
            assert(jobs(3).completed.job.index == 4)
            assert(jobs(4).completed.job.name == "5")
            assert(jobs(4).completed.job.index == 5)
          }.void
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("18.mode codec") {
    val seq = """ "sequential" """
    val par = """ "parallel-03" """
    assert(decode[BatchMode](seq).isRight)
    assert(decode[BatchMode](par).isRight)
  }

  test("19.empty sequential") {
    val se = service
      .eventStreamR(_.batch("b").sequential[Int]().batchValue(JobHook.noop))
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("20.empty parallel") {
    val se = service
      .eventStreamR(_.batch("b").parallel[Int](1)().batchValue(JobHook.noop))
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("21.monadic flatMap limits") {
    val se = service.updateConfig(_.withMetricsReport(_.fixedDelay(1.hour))).eventStreamR { agent =>
      agent.batch("many flatmap").monadic { job =>
        List.fill(5_000)(job("a", IO(1))).reduce((a, b) => a.flatMap(_ => b)).monadicBatch(JobHook.noop)
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }
}
