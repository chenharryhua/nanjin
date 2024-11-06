package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxTuple2Semigroupal
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.action.BatchMode
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class BatchTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("quasi").service("quasi").updateConfig(_.withMetricReport(Policy.crontab(_.secondly)))

  test("1.quasi.sequential") {
    service.eventStream { ga =>
      ga.batch("quasi.sequential")
        .namedSequential(
          "a" -> IO.raiseError(new Exception()),
          "b" -> IO.sleep(1.second),
          "c" -> IO.sleep(2.seconds),
          "d" -> IO.raiseError(new Exception()),
          "e" -> IO.sleep(1.seconds),
          "f" -> IO.raiseError(new Exception)
        )
        .quasi
        .map { qr =>
          assert(!qr.head.details.head.done)
          assert(qr.head.details(1).done)
          assert(qr.head.details(2).done)
          assert(!qr.head.details(3).done)
          assert(qr.head.details(4).done)
          assert(!qr.head.details(5).done)
          qr
        }
        .use(qr => IO.println(qr.asJson))
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("2.quasi.parallel") {
    service.eventStream { ga =>
      ga.batch("quasi.parallel")
        .namedParallel(3)(
          "a" -> IO.sleep(3.second),
          "b" -> IO.sleep(2.seconds),
          "c" -> IO.raiseError(new Exception),
          "d" -> IO.sleep(3.seconds),
          "e" -> IO.raiseError(new Exception),
          "f" -> IO.sleep(4.seconds)
        )
        .quasi
        .map { qr =>
          assert(qr.head.details.head.done)
          assert(qr.head.details(1).done)
          assert(!qr.head.details(2).done)
          assert(qr.head.details(3).done)
          assert(!qr.head.details(4).done)
          assert(qr.head.details(5).done)
          qr
        }
        .use(qr => IO.println(qr.asJson))
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("3.sequential") {
    service.eventStream { ga =>
      ga.batch("sequential").sequential(IO.sleep(1.second), IO.sleep(2.seconds), IO.sleep(1.seconds)).run.use_
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("4.parallel") {
    service.eventStream { ga =>
      ga.batch("parallel")
        .parallel(3)(IO.sleep(3.second), IO.sleep(2.seconds), IO.sleep(3.seconds), IO.sleep(4.seconds))
        .run
        .use_
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("5.sequential.exception") {
    service.eventStream { ga =>
      ga.batch("sequential")
        .sequential(
          IO.sleep(1.second),
          IO.sleep(2.seconds),
          IO.raiseError(new Exception),
          IO.sleep(1.seconds))
        .run
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
      ga.batch("parallel").namedParallel(3)(jobs*).run.use_
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("7.batch mode") {
    val j1 = service
      .eventStream(
        _.batch("parallel-1")
          .parallel(IO(0))
          .quasi
          .map(r => assert(r.head.mode == BatchMode.Parallel(1)))
          .use_)
      .map(checkJson)
      .evalTap(console.text[IO])
      .compile
      .drain

    val j2 = service
      .eventStream(ga =>
        ga.batch("sequential")
          .sequential(IO(0))
          .quasi
          .map(r => assert(r.head.mode == BatchMode.Sequential))
          .use_)
      .map(checkJson)
      .evalTap(console.text[IO])
      .compile
      .drain
    (j1 >> j2).unsafeRunSync()
  }

  test("8. combine") {
    service
      .updateConfig(_.withMetricReport(_.giveUp))
      .eventStream { agent =>
        val jobs = List(
          "a" -> IO.sleep(1.second).flatMap(_ => agent.herald.consoleDone("done-a")),
          "b" -> IO.sleep(2.seconds).flatMap(_ => agent.herald.consoleDone("done-b")))
        val j1 = agent.batch("s1").namedSequential(jobs*)
        val j2 = agent.batch("q1").namedParallel(jobs*)
        j1.combine(j2).quasi.use(qr => agent.herald.consoleDone(qr) >> agent.adhoc.report)
      }
      .evalMap(console.text[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("9. both") {
    service
      .updateConfig(_.withMetricReport(_.giveUp))
      .eventStream { agent =>
        val jobs = List(
          "a" -> IO.sleep(1.second).flatMap(_ => agent.herald.consoleDone("done-a")),
          "b" -> IO.sleep(2.seconds).flatMap(_ => agent.herald.consoleDone("done-b")))
        val j1 = agent.batch("s1").namedSequential(jobs*)
        val j2 = agent.batch("q1").namedParallel(jobs*)
        j1.both(j2).quasi.use(qr => agent.herald.consoleDone(qr) >> agent.adhoc.report)
      }
      .evalMap(console.text[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("10. monotonic") {
    val diff = (IO.monotonic, IO.monotonic).mapN((a, b) => b - a).unsafeRunSync()
    assert(diff.toNanos > 0)
    val res = for {
      a <- IO.monotonic
      b <- IO.monotonic
      c <- IO.monotonic
    } yield ((b - a), (c - b))

    println(res.unsafeRunSync())

  }
}
