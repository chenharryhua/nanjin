package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxFlatMapOps
import cats.syntax.traverse.toTraverseOps
import com.github.chenharryhua.nanjin.common.logging.Log
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.*
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.InformationConversions

import scala.concurrent.duration.{DurationDouble, DurationInt}

class BatchTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("quasi")
      .service("quasi")
      .updateConfig(_.withMetricsReport(_.crontab(_.secondly).repeat))

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
          assert(!qr.jobs.head.completed.succeeded)
          assert(qr.jobs(1).completed.succeeded)
          assert(qr.jobs(2).completed.succeeded)
          assert(!qr.jobs(3).completed.succeeded)
          assert(qr.jobs(4).completed.succeeded)
          assert(!qr.jobs(5).completed.succeeded)
          assert(qr.jobs.map(_.completed.job.name) == List("a", "bbb", "cccc", "ddd", "ee", "f"))
          qr
        }
        .use(_ => ga.adhoc.report)
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
          assert(qr.jobs.head.completed.succeeded)
          assert(qr.jobs(1).completed.succeeded)
          assert(!qr.jobs(2).completed.succeeded)
          assert(qr.jobs(3).completed.succeeded)
          assert(!qr.jobs(4).completed.succeeded)
          assert(qr.jobs(5).completed.succeeded)
          assert(qr.jobs.map(_.completed.job.name) == List("a", "bb", "cccc", "ddd", "ee", "f"))
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
        .valueBatch(JobHook(Log.noop[IO]).universal(_ => Json.Null))
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
        .valueBatch(JobHook.noop)
        .memoizedAcquire
        .use(_.map(_.jobs.forall(_.completed.succeeded)))
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
        .valueBatch(JobHook.noop)
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
      ga.batch("parallel").parallel(3)(jobs*).valueBatch(JobHook.noop).use_
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

  test("8.monadic for comprehension") {
    val se = service.eventStream { agent =>
      agent
        .batch("monadic")
        .monadic { job =>
          for {
            a <- job("a", IO(10))
            b <- job("b", IO.sleep(1.seconds).as(20))
            _ <- job("c", agent.adhoc.report.void)
            _ <- job("d", IO.unit)
            _ <- List(1, 2, 3).traverse(job.pure)
            _ <- job("e", IO.sleep(1.seconds))
            _ <- job("f", agent.adhoc.report.void)
            c <- job("g", IO(30))
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

  test("9.invincible monadic error") {
    val se = service.eventStream { agent =>
      agent
        .batch("monadic")
        .monadic { job =>
          for {
            a <- job("a", IO(10))
            b <- job("b", IO.sleep(1.seconds).as(20))
            _ <- job("report-1", agent.adhoc.report.void)
            _ <- job.failSafe("exception", IO.raiseError[Boolean](new Exception("aaaa")))
            _ <- job("f", IO.unit)
            _ <- job("report-2", agent.adhoc.report.void)
            c <- job("c", IO(30))
          } yield a + b + c
        }
        .monadicBatch(JobHook.noop)
        .use { qr =>
          assert(qr.jobs.head.succeeded)
          assert(qr.jobs(1).succeeded)
          assert(qr.jobs(2).succeeded)
          assert(!qr.jobs(3).succeeded)
          assert(qr.jobs(4).succeeded)
          assert(qr.jobs(5).succeeded)
          assert(qr.jobs(6).succeeded)
          assert(qr.jobs.size == 7)
          agent.adhoc.report.void
        }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("10.monadic one") {
    service.eventStream { agent =>
      agent
        .batch("monadic")
        .monadic(job => job("a", IO(0)))
        .monadicBatch(JobHook.noop)
        .use(_ => agent.adhoc.report.void)
    }.compile.drain.unsafeRunSync()
  }

  test("11.monadic many") {
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

  test("12.sorted parallel") {
    val se = service.eventStream { agent =>
      agent.batch("sorted.parallel").parallel(jobs*).valueBatch(JobHook.noop).use {
        case ValueBatch(_, _, _, _, jobs) =>
          IO {
            assert(jobs.head.result == 1)
            assert(jobs(1).result == 2)
            assert(jobs(2).result == 3)
            assert(jobs(3).result == 4)
            assert(jobs(4).result == 5)
            assert(jobs.forall(_.completed.succeeded))
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

  test("13.sorted sequential") {
    val se = service.eventStream { agent =>
      agent.batch("sorted.sequential").sequential(jobs*).valueBatch(JobHook.noop).use {
        case ValueBatch(_, _, _, _, jobs) =>
          IO {
            assert(jobs.head.result == 1)
            assert(jobs(1).result == 2)
            assert(jobs(2).result == 3)
            assert(jobs(3).result == 4)
            assert(jobs(4).result == 5)
            assert(jobs.forall(_.completed.succeeded))
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

  test("14.all batch types preserve job order") {
    var sequentialResult: List[(Int, String)] = Nil
    var parallelResult: List[(Int, String)] = Nil
    var monadicResult: List[(Int, String)] = Nil

    val se = service.eventStream { agent =>
      val sequential = agent
        .batch("ordered.sequential")
        .sequential("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
        .valueBatch(JobHook.noop)
        .use { batch =>
          IO {
            sequentialResult = batch.jobs.map(j => j.completed.job.index -> j.completed.job.name)
          }
        }

      val parallel = agent
        .batch("ordered.parallel")
        .parallel(3)("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
        .valueBatch(JobHook.noop)
        .use { batch =>
          IO {
            parallelResult = batch.jobs.map(j => j.completed.job.index -> j.completed.job.name)
          }
        }

      val monadic = agent
        .batch("ordered.monadic")
        .monadic { job =>
          for {
            a <- job("a", IO(1))
            b <- job("b", IO(2))
            c <- job("c", IO(3))
          } yield a + b + c
        }
        .monadicBatch(JobHook.noop)
        .use { batch =>
          IO {
            monadicResult = batch.jobs.map(j => j.job.index -> j.job.name)
          }
        }

      sequential >> parallel >> monadic
    }.compile.lastOrError.unsafeRunSync()

    assert(sequentialResult == List((1, "a"), (2, "b"), (3, "c")))
    assert(parallelResult == List((1, "a"), (2, "b"), (3, "c")))
    assert(monadicResult == List((1, "a"), (2, "b"), (3, "c")))
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("15.empty sequential") {
    val se = service
      .eventStreamR(_.batch("b").sequential[Int]().valueBatch(JobHook.noop))
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("16.empty parallel") {
    val se = service
      .eventStreamR(_.batch("b").parallel[Int](1)().valueBatch(JobHook.noop))
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("17.monadic flatMap limits") {
    val se = service.updateConfig(_.withMetricsReport(_.fixedDelay(1.hour).repeat)).eventStreamR { agent =>
      agent.batch("many flatmap").monadic { job =>
        List.fill(5_000)(job("a", IO(1))).reduce((a, b) => a.flatMap(_ => b)).monadicBatch(JobHook.noop) >>
          (1 to 5_000).toList.traverse(x => job(x.toString, IO(x))).monadicBatch(JobHook.noop)
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("18.monadic lift(F[A]) - Batch") {
    val se = service.eventStreamR { agent =>
      agent.batch("lift").monadic { job =>
        val result = for {
          config <- job.lift(IO("hello"))
          len <- job("length", IO(config.length))
        } yield len
        result.monadicBatch(JobHook.noop).map { mb =>
          assert(mb.succeeded)
          assert(mb.result == Right(5))
          // lift does not create a job entry; only "length" appears
          assert(mb.jobs.size == 1)
          assert(mb.jobs.head.job.name == "length")
        }
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("19.monadic lift(F[A]) - BatchLight") {
    val result = service.eventStreamR { agent =>
      agent.batchLight("lift-light").monadic { job =>
        val batch = for {
          x <- job.lift(IO(42))
          y <- job("double", IO(x * 2))
        } yield y
        cats.effect.Resource.eval(batch.monadicBatch).map { mb =>
          assert(mb.succeeded)
          assert(mb.result == Right(84))
          assert(mb.jobs.size == 1)
          assert(mb.jobs.head.job.name == "double")
        }
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(result.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("20.monadic lift(F[A]) - exception crashes the batch") {
    val se = service.eventStream { agent =>
      agent.batch("lift-error").monadic { job =>
        val result = for {
          _ <- job.lift(IO.raiseError[Int](new Exception("boom")))
          _ <- job("should-not-run", IO(1))
        } yield ()
        result.monadicBatch(JobHook.noop).use_
      }
    }.map(checkJson).compile.lastOrError.unsafeRunSync()
    // lift exception is unhandled — it crashes the service (ByException)
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 3)
  }

  test("21.monadic lift(F[A]) - BatchLight - exception crashes the batch") {
    val result = service.eventStream { agent =>
      agent.batchLight("lift-light-error").monadic { job =>
        val batch = for {
          _ <- job.lift(IO.raiseError[String](new Exception("oops")))
          _ <- job("unreachable", IO(99))
        } yield ()
        batch.monadicBatch.void
      }
    }.map(checkJson).compile.lastOrError.unsafeRunSync()
    // lift exception is unhandled — it crashes the service
    assert(result.asInstanceOf[ServiceStop].cause.exitCode == 3)
  }

  test("22.monadic lift(Resource) - resource acquired and used") {
    val se = service.eventStreamR { agent =>
      agent.batch("lift-resource").monadic { job =>
        val result = for {
          ref <- job.lift(cats.effect.Resource.eval(cats.effect.Ref[IO].of(0)))
          _ <- job("increment", ref.update(_ + 1))
          _ <- job("increment2", ref.update(_ + 10))
          v <- job("read", ref.get)
        } yield v
        result.monadicBatch(JobHook.noop).map { mb =>
          assert(mb.succeeded)
          assert(mb.result == Right(11))
          assert(mb.jobs.size == 3)
          assert(mb.jobs.map(_.job.name) == List("increment", "increment2", "read"))
        }
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("23.monadic lift(Resource) - acquisition failure crashes the batch") {
    val se = service.eventStream { agent =>
      agent.batch("lift-resource-error").monadic { job =>
        val result = for {
          _ <- job.lift(cats.effect.Resource.raiseError[IO, Int, Throwable](new Exception("acquire fail")))
          _ <- job("unreachable", IO(1))
        } yield ()
        result.monadicBatch(JobHook.noop).use_
      }
    }.map(checkJson).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 3)
  }
}
