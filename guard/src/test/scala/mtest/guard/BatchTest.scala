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
          "a" -> IO.raiseError[Boolean](new Exception()),
          "bbb" -> IO.sleep(1.second).map(_ => true),
          "cccc" -> IO.sleep(2.seconds).map(_ => true),
          "ddd" -> IO.raiseError(new Exception()),
          "ee" -> IO.sleep(1.seconds).map(_ => true),
          "f" -> IO.raiseError(new Exception)
        )
        .quasi
        .map { qr =>
          assert(!qr.details.head.done)
          assert(qr.details(1).done)
          assert(qr.details(2).done)
          assert(!qr.details(3).done)
          assert(qr.details(4).done)
          assert(!qr.details(5).done)
          qr
        }
        .use(qr => IO.println(qr.asJson) *> ga.adhoc.report)
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("2.quasi.parallel") {
    service.eventStream { ga =>
      ga.batch("quasi.parallel")
        .namedParallel(3)(
          "a" -> IO.sleep(3.second).map(_ => true),
          "bb" -> IO.sleep(2.seconds).map(_ => true),
          "cccc" -> IO.raiseError(new Exception),
          "ddd" -> IO.sleep(3.seconds).map(_ => true),
          "ee" -> IO.raiseError(new Exception),
          "f" -> IO.sleep(4.seconds).map(_ => true)
        )
        .quasi
        .map { qr =>
          assert(qr.details.head.done)
          assert(qr.details(1).done)
          assert(!qr.details(2).done)
          assert(qr.details(3).done)
          assert(!qr.details(4).done)
          assert(qr.details(5).done)
          qr
        }
        .use(qr => IO.println(qr.asJson) *> ga.adhoc.report)
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("3.sequential") {
    service.eventStream { ga =>
      ga.batch("sequential")
        .sequential(IO.sleep(1.second), IO.sleep(2.seconds), IO.sleep(1.seconds))
        .fully
        .use_
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("4.parallel") {
    service.eventStream { ga =>
      ga.batch("parallel")
        .parallel(3)(IO.sleep(3.second), IO.sleep(2.seconds), IO.sleep(3.seconds), IO.sleep(4.seconds))
        .fully
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
        .fully
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
      ga.batch("parallel").namedParallel(3)(jobs*).fully.use_
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("7.batch mode") {
    val j1 = service
      .eventStream(
        _.batch("parallel-1").parallel(IO(true)).quasi.map(r => assert(r.mode == BatchMode.Parallel(1))).use_)
      .map(checkJson)
      .evalTap(console.text[IO])
      .compile
      .drain

    val j2 = service
      .eventStream(ga =>
        ga.batch("sequential")
          .sequential(IO(true))
          .quasi
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

  test("9. single") {
    service.eventStream { agent =>
      agent
        .batch("single")
        .single
        .nextJob(_ => IO.println("a"))
        .nextJob(IO.println("b"))
        .nextJob("report", agent.adhoc.report)
        .nextJob("c", _ => IO.println("c"))
        .fully
        .use(_ => agent.adhoc.report)
    }.evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("10. single quasi") {
    service.eventStream { agent =>
      agent
        .batch("single")
        .single
        .nextJob("a", _ => IO.println("a"))
        .nextJob("b" -> IO.println("b"))
        .nextJob("exception", IO.raiseError[Int](new Exception))
        .nextJob("c", _ => IO.println("c"))
        .quasi
        .use(qr =>
          agent.adhoc.report >>
            agent.console.done(qr) >>
            IO {
              assert(qr.details.head.done)
              assert(qr.details(1).done)
              assert(!qr.details(2).done)
              assert(!qr.details(3).done)
              ()
            })
    }.evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }
}
