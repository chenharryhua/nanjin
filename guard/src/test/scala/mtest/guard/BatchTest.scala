package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxFlatMapOps
import cats.syntax.traverse.toTraverseOps
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.*
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.InformationConversions

import scala.concurrent.duration.{DurationDouble, DurationInt}

class BatchTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("quasi")
      .service("quasi")
      .updateConfig(_.withReportPolicy(_.crontab(_.secondly).repeat))

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
        .quasiBatch
        .map { qr =>
          assert(!qr.outcomes.head.record.succeeded)
          assert(qr.outcomes(1).record.succeeded)
          assert(qr.outcomes(2).record.succeeded)
          assert(!qr.outcomes(3).record.succeeded)
          assert(qr.outcomes(4).record.succeeded)
          assert(!qr.outcomes(5).record.succeeded)
          assert(qr.outcomes.map(_.record.job.name) == List("a", "bbb", "cccc", "ddd", "ee", "f"))
          qr
        }
        .use(b => ga.logger.good(b) >> ga.adhoc.report)
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
        .quasiBatch
        .map { qr =>
          assert(qr.outcomes.head.record.succeeded)
          assert(qr.outcomes(1).record.succeeded)
          assert(!qr.outcomes(2).record.succeeded)
          assert(qr.outcomes(3).record.succeeded)
          assert(!qr.outcomes(4).record.succeeded)
          assert(qr.outcomes(5).record.succeeded)
          assert(qr.outcomes.map(_.record.job.name) == List("a", "bb", "cccc", "ddd", "ee", "f"))
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
          "a" -> IO.sleep(1.second).as(1.mb.toString),
          "b" -> IO.sleep(2.seconds).as(2.tb.toString),
          "c" -> IO.sleep(1.seconds).as(3.bytes.toString))
        .valueBatch
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
        .valueBatch
        .memoizedAcquire
        .use(_.map(_.outcomes.forall(_.record.succeeded)))
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
        .valueBatch
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
      ga.batch("parallel").parallel(3)(jobs*).valueBatch.use_
    }.map(checkJson).compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 3)

  }

  test("7.batch mode") {
    val j1 = service
      .eventStream(
        _.batch("parallel-1")
          .parallel("a" -> IO(true))
          .quasiBatch
          .map(r => assert(r.mode == BatchMode.Parallel(1)))
          .use_)
      .map(checkJson)
      .compile
      .drain

    val j2 = service
      .eventStream(ga =>
        ga.batch("sequential")
          .sequential("a" -> IO(true))
          .quasiBatch
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
        .monadicBatch
        .use { qr =>
          assert(qr.result == Right(60))
          assert(qr.outcomes.map(_.record.job.name) == List("a", "b", "c", "d", "e", "f", "g"))
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
            _ <- job("rejected", IO(0), _ => false)
            _ <- job("f", IO.unit)
            _ <- job("report-2", agent.adhoc.report.void)
            c <- job("c", IO(30))
          } yield a + b + c
        }
        .monadicBatch
        .use { qr =>
          assert(qr.outcomes.head.record.succeeded)
          assert(qr.outcomes(1).record.succeeded)
          assert(qr.outcomes(2).record.succeeded)
          assert(!qr.outcomes(3).record.succeeded)
          assert(qr.outcomes(4).record.succeeded)
          assert(qr.outcomes(5).record.succeeded)
          assert(qr.outcomes(6).record.succeeded)
          assert(qr.outcomes.size == 7)
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
        .monadicBatch
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
        .monadicBatch
        .use { qr =>
          val details = qr.outcomes
          assert(details.head.record.job.name === "1")
          assert(details.head.record.job.index === 1)
          assert(details(1).record.job.name === "2")
          assert(details(1).record.job.index === 2)
          assert(details(2).record.job.name === "3")
          assert(details(2).record.job.index === 3)
          assert(details(3).record.job.name === "10")
          assert(details(3).record.job.index === 4)
          assert(details(4).record.job.name === "20")
          assert(details(4).record.job.index === 5)
          assert(details(5).record.job.name === "30")
          assert(details(5).record.job.index === 6)
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
      agent.batch("sorted.parallel").parallel(jobs*).valueBatch.use {
        case ValueBatch(_, _, _, _, outcomes, values) =>
          IO {
            assert(values.head == 1)
            assert(values(1) == 2)
            assert(values(2) == 3)
            assert(values(3) == 4)
            assert(values(4) == 5)
            assert(outcomes.forall(_.record.succeeded))
            assert(outcomes.head.record.job.name == "1")
            assert(outcomes.head.record.job.index == 1)
            assert(outcomes(1).record.job.name == "2")
            assert(outcomes(1).record.job.index == 2)
            assert(outcomes(2).record.job.name == "3")
            assert(outcomes(2).record.job.index == 3)
            assert(outcomes(3).record.job.name == "4")
            assert(outcomes(3).record.job.index == 4)
            assert(outcomes(4).record.job.name == "5")
            assert(outcomes(4).record.job.index == 5)
          }.void
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("13.sorted sequential") {
    val se = service.eventStream { agent =>
      agent.batch("sorted.sequential").sequential(jobs*).valueBatch.use {
        case ValueBatch(_, _, _, _, outcomes, values) =>
          IO {
            assert(values.head == 1)
            assert(values(1) == 2)
            assert(values(2) == 3)
            assert(values(3) == 4)
            assert(values(4) == 5)
            assert(outcomes.forall(_.record.succeeded))
            assert(outcomes.head.record.job.name == "1")
            assert(outcomes.head.record.job.index == 1)
            assert(outcomes(1).record.job.name == "2")
            assert(outcomes(1).record.job.index == 2)
            assert(outcomes(2).record.job.name == "3")
            assert(outcomes(2).record.job.index == 3)
            assert(outcomes(3).record.job.name == "4")
            assert(outcomes(3).record.job.index == 4)
            assert(outcomes(4).record.job.name == "5")
            assert(outcomes(4).record.job.index == 5)
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
        .valueBatch
        .use { batch =>
          IO {
            sequentialResult = batch.outcomes.map(j => j.record.job.index -> j.record.job.name)
          }
        }

      val parallel = agent
        .batch("ordered.parallel")
        .parallel(3)("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
        .valueBatch
        .use { batch =>
          IO {
            parallelResult = batch.outcomes.map(j => j.record.job.index -> j.record.job.name)
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
        .monadicBatch
        .use { batch =>
          IO {
            monadicResult = batch.outcomes.map(j => j.record.job.index -> j.record.job.name)
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
      .eventStreamR(_.batch("b").sequential[Int]().valueBatch)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("16.empty parallel") {
    val se = service
      .eventStreamR(_.batch("b").parallel[Int](1)().valueBatch)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("17.monadic flatMap limits") {
    val se = service.updateConfig(_.withReportPolicy(_.fixedDelay(1.hour).repeat)).eventStreamR { agent =>
      agent.batch("many flatmap").monadic { job =>
        List.fill(5_000)(job("a", IO(1))).reduce((a, b) => a.flatMap(_ => b)).monadicBatch >>
          (1 to 5_000).toList.traverse(x => job(x.toString, IO(x))).monadicBatch
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("18.monadic lift(F[A]) - Batch") {
    val se = service.eventStreamR { agent =>
      agent.batch("lift").monadic { job =>
        val result = for {
          config <- job.untracked(IO("hello"))
          len <- job("length", IO(config.length))
        } yield len
        result.monadicBatch.map { mb =>
          assert(mb.result.isRight)
          assert(mb.result == Right(5))
          // untracked does not create a job entry; only "length" appears
          assert(mb.outcomes.size == 1)
          assert(mb.outcomes.head.record.job.name == "length")
        }
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("19.monadic lift(F[A]) - BatchLight") {
    val result = service.eventStreamR { agent =>
      agent.batchLight("lift-light").monadic { job =>
        val batch = for {
          x <- job.untracked(IO(42))
          y <- job("double", IO(x * 2))
        } yield y
        cats.effect.Resource.eval(batch.monadicBatch).map { mb =>
          assert(mb.result.isRight)
          assert(mb.result == Right(84))
          assert(mb.outcomes.size == 1)
          assert(mb.outcomes.head.record.job.name == "double")
        }
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(result.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("20.monadic lift(F[A]) - failure short-circuits the chain") {
    val se = service.eventStreamR { agent =>
      agent.batch("lift-error").monadic { job =>
        val boom = new Exception("boom")
        val result = for {
          _ <- job.untracked(IO.raiseError[Int](boom))
          _ <- job("should-not-run", IO(1))
        } yield ()
        result.monadicBatch.map { mb =>
          // the lifted failure surfaces as Left; no further job runs
          assert(mb.result.isLeft)
          assert(mb.outcomes.isEmpty)
          // an untracked-step failure is wrapped so it is distinguishable from a tracked job's failure,
          // and the original exception is preserved as the cause
          mb.result.left.toOption.get match {
            case UntrackedStepException(cause) => assert(cause eq boom)
            case other                         => fail(s"expected UntrackedStepException, got $other")
          }
        }
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("21.monadic lift(F[A]) - BatchLight - failure short-circuits the chain") {
    val se = service.eventStream { agent =>
      agent.batchLight("lift-light-error").monadic { job =>
        val oops = new Exception("oops")
        val batch = for {
          _ <- job.untracked(IO.raiseError[String](oops))
          _ <- job("unreachable", IO(99))
        } yield ()
        batch.monadicBatch.map { mb =>
          assert(mb.result.isLeft)
          assert(mb.outcomes.isEmpty)
          // BatchLight wraps untracked-step failures identically to Batch
          mb.result.left.toOption.get match {
            case UntrackedStepException(cause) => assert(cause eq oops)
            case other                         => fail(s"expected UntrackedStepException, got $other")
          }
        }.void
      }
    }.map(checkJson).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("22.monadic lift(Resource) - resource acquired and used") {
    val se = service.eventStreamR { agent =>
      agent.batch("lift-resource").monadic { job =>
        val result = for {
          ref <- job.untracked(cats.effect.Resource.eval(cats.effect.Ref[IO].of(0)))
          _ <- job("increment", ref.update(_ + 1))
          _ <- job("increment2", ref.update(_ + 10))
          v <- job("read", ref.get)
        } yield v
        result.monadicBatch.map { mb =>
          assert(mb.result.isRight)
          assert(mb.result == Right(11))
          assert(mb.outcomes.size == 3)
          assert(mb.outcomes.map(_.record.job.name) == List("increment", "increment2", "read"))
        }
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("23.monadic lift(Resource) - acquisition failure short-circuits the chain") {
    val se = service.eventStreamR { agent =>
      agent.batch("lift-resource-error").monadic { job =>
        val result = for {
          _ <- job.untracked(
            cats.effect.Resource.raiseError[IO, Int, Throwable](new Exception("acquire fail")))
          _ <- job("unreachable", IO(1))
        } yield ()
        result.monadicBatch.map { mb =>
          assert(mb.result.isLeft)
          assert(mb.outcomes.isEmpty)
        }
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("24.monadic spent counts invisible lift steps between jobs") {
    // Regression: the old spent summed per-job took, dropping wall-clock consumed by
    // invisible lift/pure steps. spent is now the full span, so a 200ms lifted sleep
    // between two fast jobs must be reflected in spent.
    val se = service.eventStreamR { agent =>
      agent.batch("monadic-invisible-lift").monadic { job =>
        val result = for {
          a <- job("a", IO(1))
          _ <- job.untracked(IO.sleep(200.millis))
          b <- job("b", IO(2))
        } yield a + b
        result.monadicBatch.map { mb =>
          assert(mb.result == Right(3))
          assert(mb.outcomes.map(_.record.job.name) == List("a", "b"))
          assert(mb.spent.toMillis >= 200L)
        }
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("25.monadic sum of per-job took within spent (gaps redistributed)") {
    // monadicHistory rewrites each job's start to the previous job's end, so per-job took
    // values are contiguous and telescope to the span from the first job's start to the last
    // job's end. spent is measured against a fresh clock reading taken after the whole chain
    // finishes, so it also covers trailing framing that follows the last job; sumTook is
    // therefore <= spent, with only a tiny remainder.
    //
    // Alignment guard: Batch and BatchLight share the same timing model. This relationship
    // must hold identically here and in BatchLightMonadicTest "sum of per-job took telescopes
    // to the span through the last job". If one changes, both must — do not let the two
    // variants drift apart.
    val se = service.eventStreamR { agent =>
      agent.batch("monadic-took-sum").monadic { job =>
        val result = for {
          a <- job("a", IO.sleep(30.millis).as(1))
          _ <- job.pure(())
          _ <- job.untracked(IO.sleep(80.millis))
          b <- job("b", IO.sleep(30.millis).as(2))
          c <- job("c", IO.sleep(30.millis).as(3))
        } yield a + b + c
        result.monadicBatch.map { mb =>
          assert(mb.result == Right(6))
          val sumTook = mb.outcomes.map(_.record.took.toNanos).sum
          assert(sumTook <= mb.spent.toNanos)
          // the trailing remainder is bookkeeping only, far below the ~170ms of real work
          assert(mb.spent.toNanos - sumTook < 50_000_000L) // 50ms
        }
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("26.single-job monadic batch spent covers that job's took, within a tiny remainder") {
    // Edge of monadicHistory: with one visible job there is nothing to redistribute, so the
    // single job's took is the whole through-last-job span. spent adds only the trailing
    // framing captured by the fresh post-chain reading (here also the metrics-panel
    // deactivation), so took <= spent by a tiny margin.
    //
    // Alignment guard: mirrors BatchLightMonadicTest "single-job monadic batch spent covers
    // that job's took". Batch and BatchLight must agree on this edge of the timing model.
    val se = service.eventStreamR { agent =>
      agent.batch("monadic-single-job").monadic { job =>
        job("only", IO.sleep(40.millis).as(1)).monadicBatch.map { mb =>
          assert(mb.outcomes.size == 1)
          assert(mb.outcomes.head.record.took.toNanos <= mb.spent.toNanos)
          assert(mb.spent.toNanos - mb.outcomes.head.record.took.toNanos < 50_000_000L) // 50ms
          assert(mb.spent.toMillis >= 40L)
        }
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("27.monadic untracked(Resource) release failure surfaces through the scope") {
    // Guards the documented boundary: acquisition failure is captured into `result`,
    // but a *release* failure is not — it surfaces through the resource scope. Here the
    // untracked resource acquires fine and every job succeeds (so mb.result is Right),
    // yet releasing it on scope close throws, which fails the effect that ran the batch.
    @volatile var observedResult: Option[Either[Throwable, Int]] = None
    val se = service.eventStream { agent =>
      agent.batch("release-fail").monadic { job =>
        val result = for {
          _ <- job.untracked(
            cats.effect.Resource.make(IO.unit)(_ => IO.raiseError(new Exception("release fail"))))
          v <- job("ok", IO(1))
        } yield v
        result.monadicBatch.use { mb =>
          IO { observedResult = Some(mb.result) }
        }
      }
    }.map(checkJson).compile.lastOrError.unsafeRunSync()
    // inside the scope the batch succeeded ...
    assert(observedResult == Some(Right(1)))
    // ... yet the release fault brought the service down (uncaught in F)
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 3)
  }
}
