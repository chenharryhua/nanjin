package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxTuple2Semigroupal
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.action.{Batch, BatchMode, MeasuredValue, TraceJob}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import squants.DimensionlessConversions.DimensionlessConversions
import squants.information.InformationConversions.InformationConversions

import scala.concurrent.duration.{DurationDouble, DurationInt}

class BatchTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("quasi")
      .service("quasi")
      .updateConfig(_.withMetricReport(Policy.crontab(_.secondly)).withAlarmLevel(_.Debug))

  test("1.quasi.sequential") {
    service.eventStream { ga =>
      ga.batch("quasi.sequential")
        .sequential[Unit](
          "a" -> IO.raiseError(new Exception()),
          "bbb" -> IO.sleep(1.second),
          "cccc" -> IO.sleep(2.seconds),
          "ddd" -> IO.raiseError(new Exception()),
          "ee" -> IO.sleep(1.seconds),
          "f" -> IO.raiseError(new Exception)
        )
        .map(_ => true)
        .renameJobs(_ + ":test")
        .quasiBatch(TraceJob.noop)(identity)
        .map { qr =>
          assert(!qr.jobs.head.done)
          assert(qr.jobs(1).done)
          assert(qr.jobs(2).done)
          assert(!qr.jobs(3).done)
          assert(qr.jobs(4).done)
          assert(!qr.jobs(5).done)
          qr
        }
        .use(qr => IO.println(qr.asJson) *> ga.adhoc.report)
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("2.quasi.parallel") {
    service.eventStream { ga =>
      ga.batch("quasi.parallel")
        .parallel(3)(
          "a" -> IO.sleep(3.second),
          "bb" -> IO.sleep(2.seconds),
          "cccc" -> IO.raiseError(new Exception),
          "ddd" -> IO.sleep(3.seconds),
          "ee" -> IO.raiseError(new Exception),
          "f" -> IO.sleep(4.seconds)
        )
        .renameJobs(_ + ":test")
        .quasiBatch(
          TraceJob.generic(ga, _.sendSuccessTo(_.void).sendKickoffTo(_.void).sendFailureTo(_.void)))(_ =>
          true)
        .map { qr =>
          assert(qr.jobs.head.done)
          assert(qr.jobs(1).done)
          assert(!qr.jobs(2).done)
          assert(qr.jobs(3).done)
          assert(!qr.jobs(4).done)
          assert(qr.jobs(5).done)
          qr
        }
        .use(_ => ga.adhoc.report)
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("3.sequential") {
    service.eventStream { agent =>
      agent
        .batch("sequential")
        .sequential(
          "a" -> IO.sleep(1.second).as(1.mb),
          "b" -> IO.sleep(2.seconds).as(2.tb),
          "c" -> IO.sleep(1.seconds).as(3.bytes))
        .measuredValue(TraceJob.dataRate(agent))
        .use_
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("4.parallel") {
    val se = service.eventStream { ga =>
      ga.batch("parallel")
        .parallel(3)(
          "a" -> IO.sleep(3.second),
          "b" -> IO.sleep(2.seconds),
          "c" -> IO.sleep(3.seconds),
          "d" -> IO.sleep(4.seconds))
        .map(_ => true)
        .measuredValue(TraceJob.json(ga).contramap(_.asJson))
        .memoizedAcquire
        .use(_.map(_.batch.jobs.forall(_.done)))
        .map(assert(_))
        .void
    }.map(checkJson).evalTap(console.text[IO]).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("5.sequential.exception") {
    service.eventStream { ga =>
      ga.batch("sequential")
        .sequential(
          "a" -> IO.sleep(1.second),
          "b" -> IO.sleep(2.seconds),
          "c" -> IO.raiseError(new Exception),
          "d" -> IO.sleep(1.seconds))
        .measuredValue(TraceJob.noop)
        .use_
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("6.parallel.exception") {
    val jobs = List(
      "a" -> IO.sleep(1.second),
      "b" -> IO.sleep(2.seconds),
      "c" -> IO.sleep(3.seconds),
      "d" -> (IO.sleep(3.seconds) >> IO.raiseError(new Exception)),
      "e" -> IO.sleep(4.seconds)
    )
    service.eventStream { ga =>
      ga.batch("parallel").parallel(3)(jobs*).measuredValue(TraceJob.noop).use_
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("7.batch mode") {
    val j1 = service
      .eventStream(
        _.batch("parallel-1")
          .parallel("a" -> IO(true))
          .quasiBatch(TraceJob.noop)(a => a)
          .map(r => assert(r.mode == BatchMode.Parallel(1)))
          .use_)
      .map(checkJson)
      .evalTap(console.text[IO])
      .compile
      .drain

    val j2 = service
      .eventStream(ga =>
        ga.batch("sequential")
          .sequential("a" -> IO(true))
          .quasiBatch(TraceJob.noop)(a => a)
          .map(r => assert(r.mode == BatchMode.Sequential))
          .use_)
      .map(checkJson)
      .evalTap(console.text[IO])
      .compile
      .drain
    (j1 >> j2).unsafeRunSync()
  }

  test("8. monotonic") {
    val diff = (IO.monotonic, IO.monotonic).mapN((a, b) => b - a).unsafeRunSync()
    assert(diff.toNanos > 0)
    val res = for {
      a <- IO.monotonic
      b <- IO.monotonic
      c <- IO.monotonic
    } yield (b - a, c - b)

    println(res.unsafeRunSync())
  }

  test("9. guarantee") {
    assertThrows[Exception](IO(1).guarantee(IO.raiseError(new Exception())).unsafeRunSync())
  }

  test("10. bracket") {
    assertThrows[Exception](
      IO.bracketFull(_ => IO(1))(IO.println)((_, _) => IO.raiseError(new Exception())).unsafeRunSync())
  }

  test("11. monadic") {
    service.eventStream { agent =>
      agent.batch("monadic").monadic { job =>
        job("a", IO.println(1))
          .flatMap(_ => job("b", IO.println(2)))
          .flatMap(_ => job("c", agent.adhoc.report >> IO.sleep(1.seconds)))
          .flatMap(_ => job("d", IO.println(4)))
          .flatMap(_ => job("e", agent.adhoc.report))
          .flatMap(_ => job("f", IO.println(6)))
          .quasiBatch(TraceJob.json(agent))
          .use(agent.herald.done(_) >> agent.adhoc.report)
      }
    }.evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("12. monadic for comprehension") {
    val se = service.eventStream { agent =>
      agent
        .batch("monadic")
        .monadic { job =>
          for {
            a <- job("a", IO.println("a").as(10))
            b <- job("b", IO.sleep(1.seconds) >> IO.println("b").as(20))
            _ <- job("c", agent.adhoc.report)
            _ <- job("d" -> IO.println("aaaa"))
            _ <- job("e" -> IO.sleep(1.seconds).flatMap(_ => IO.println("bbbb")))
            _ <- job("f" -> agent.adhoc.report)
            c <- job("g", IO.println("c").as(30))
          } yield a + b + c
        }
        .measuredValue(TraceJob.json(agent))
        .use { qr =>
          assert(qr.value == 60)
          agent.adhoc.report
        }
    }.evalTap(console.text[IO]).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)

  }

  test("13. monadic error") {
    val se = service.eventStream { agent =>
      agent
        .batch("monadic")
        .monadic { job =>
          for {
            a <- job("a", IO.println("a").as(10))
            b <- job("b", IO.sleep(1.seconds) >> IO.println("b").as(20))
            _ <- job("report-1" -> agent.adhoc.report)
            _ <- job("exception", IO.raiseError[Unit](new Exception("aaaa")))
            _ <- job("f" -> (IO.sleep(1000.seconds) >> IO.println("bbbb")))
            _ <- job("report-2" -> agent.adhoc.report)
            c <- job("c", IO.println("c").as(30))
          } yield a + b + c
        }
        .quasiBatch(TraceJob.json(
          agent,
          _.sendKickoffTo(_.herald.info).sendSuccessTo(_.console.done).sendFailureTo(_.console.warn)))
        .use { qr =>
          assert(qr.jobs.head.done)
          assert(qr.jobs(1).done)
          assert(qr.jobs(2).done)
          assert(!qr.jobs(3).done)
          assert(qr.jobs.size == 4)
          agent.adhoc.report >> agent.herald.info(qr)
        }
    }.evalTap(console.text[IO]).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("14. monadic one") {
    service.eventStream { agent =>
      agent
        .batch("monadic")
        .monadic(job => job("a" -> IO(0)))
        .measuredValue(TraceJob.json(agent))
        .use(qr => agent.adhoc.report >> agent.herald.info(qr.batch))
    }.evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("15. monadic many") {
    val se = service.eventStream { agent =>
      agent
        .batch("monadic")
        .monadic { (job: Batch.JobBuilder[IO]) =>
          val p1 = for {
            a <- job("1" -> IO(1))
            b <- job("2" -> IO(2))
            c <- job("3" -> IO(3))
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
        .quasiBatch(TraceJob.json(agent, _.sendKickoffTo(_.herald.info).sendSuccessTo(_.console.info)))
        .use { qr =>
          val details = qr.jobs
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
          agent.adhoc.report >> agent.herald.info(qr)
        }
    }.evalTap(console.text[IO]).compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  private val jobs: List[(String, IO[Int])] = List(
    "1" -> IO(1).delayBy(3.second),
    "2" -> IO(2).delayBy(2.second),
    "3" -> IO(3).delayBy(2.second),
    "4" -> IO(4).delayBy(1.second),
    "5" -> IO(5).delayBy(0.1.second)
  )

  test("17. sorted parallel") {
    val se = service.eventStream { agent =>
      agent
        .batch("sorted.parallel")
        .parallel(jobs*)
        .measuredValue(TraceJob.dataRate(agent).contramap(_.kb))
        .use { case MeasuredValue(br, rst) =>
          IO {
            assert(rst.head == 1)
            assert(rst(1) == 2)
            assert(rst(2) == 3)
            assert(rst(3) == 4)
            assert(rst(4) == 5)
            assert(br.jobs.forall(_.done))
            assert(br.jobs.head.job.name == "1")
            assert(br.jobs.head.job.index == 1)
            assert(br.jobs(1).job.name == "2")
            assert(br.jobs(1).job.index == 2)
            assert(br.jobs(2).job.name == "3")
            assert(br.jobs(2).job.index == 3)
            assert(br.jobs(3).job.name == "4")
            assert(br.jobs(3).job.index == 4)
            assert(br.jobs(4).job.name == "5")
            assert(br.jobs(4).job.index == 5)
          }.void
        }
    }.evalTap(console.text[IO]).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("18. sorted sequential") {
    val se = service.eventStream { agent =>
      agent
        .batch("sorted.sequential")
        .sequential(jobs*)
        .measuredValue(TraceJob.scalarRate(agent).contramap(x => (x * 10.0d).each))
        .use { case MeasuredValue(br, rst) =>
          IO {
            assert(rst.head == 1)
            assert(rst(1) == 2)
            assert(rst(2) == 3)
            assert(rst(3) == 4)
            assert(rst(4) == 5)
            assert(br.jobs.forall(_.done))
            assert(br.jobs.head.job.name == "1")
            assert(br.jobs.head.job.index == 1)
            assert(br.jobs(1).job.name == "2")
            assert(br.jobs(1).job.index == 2)
            assert(br.jobs(2).job.name == "3")
            assert(br.jobs(2).job.index == 3)
            assert(br.jobs(3).job.name == "4")
            assert(br.jobs(3).job.index == 4)
            assert(br.jobs(4).job.name == "5")
            assert(br.jobs(4).job.index == 5)
          }.void
        }
    }.evalTap(console.text[IO]).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("19. mode codec") {
    val seq = """ "sequential" """
    val par = """ "parallel-03" """
    assert(decode[BatchMode](seq).isRight)
    assert(decode[BatchMode](par).isRight)
  }

  test("20. empty sequential") {
    val se = service
      .eventStream(_.batch("b").sequential[Int]().measuredValue(TraceJob.noop).use_)
      .evalTap(console.text[IO])
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("21. empty parallel") {
    val se = service
      .eventStream(_.batch("b").parallel[Int](1)().measuredValue(TraceJob.noop).use_)
      .evalTap(console.text[IO])
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }
}
