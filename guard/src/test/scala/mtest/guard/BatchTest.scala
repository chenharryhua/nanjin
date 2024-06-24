package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class BatchTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("quasi").service("quasi").updateConfig(_.withMetricReport(policies.crontab(_.secondly)))

  test("1.quasi.sequential") {
    service.eventStream { ga =>
      ga.batch("quasi.sequential", _.timed.counted.bipartite.withMeasurement("batch-seq-quasi"))
        .quasi
        .namedSequential(
          "a" -> IO.raiseError(new Exception()),
          "b" -> IO.sleep(1.second),
          "c" -> IO.sleep(2.seconds),
          "d" -> IO.raiseError(new Exception()),
          "e" -> IO.sleep(1.seconds),
          "f" -> IO.raiseError(new Exception)
        )
        .map { qr =>
          assert(!qr.details.head.is_done)
          assert(qr.details(1).is_done)
          assert(qr.details(3).is_done)
          assert(!qr.details(4).is_done)
          assert(qr.details(5).is_done)
          assert(!qr.details(6).is_done)
        }
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("2.quasi.parallel") {
    service.eventStream { ga =>
      ga.batch("quasi.parallel", _.timed.counted.bipartite.policy(policies.fixedDelay(1.second).limited(1)))
        .quasi
        .namedParallel(3)(
          "a" -> IO.sleep(3.second),
          "b" -> IO.sleep(2.seconds),
          "c" -> IO.raiseError(new Exception),
          "d" -> IO.sleep(3.seconds),
          "e" -> IO.raiseError(new Exception),
          "f" -> IO.sleep(4.seconds)
        )
        .map { qr =>
          assert(qr.details.head.is_done)
          assert(qr.details(1).is_done)
          assert(!qr.details(2).is_done)
          assert(qr.details(3).is_done)
          assert(!qr.details(4).is_done)
          assert(qr.details(5).is_done)
        }
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("3.sequential") {
    service.eventStream { ga =>
      ga.batch("sequential", _.timed.counted.bipartite.withMeasurement("batch-seq"))
        .sequential(IO.sleep(1.second), IO.sleep(2.seconds), IO.sleep(1.seconds))
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("4.parallel") {
    service.eventStream { ga =>
      ga.batch("parallel", _.timed.counted.bipartite)
        .parallel(3)(IO.sleep(3.second), IO.sleep(2.seconds), IO.sleep(3.seconds), IO.sleep(4.seconds))
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("5.sequential.exception") {
    service.eventStream { ga =>
      ga.batch("sequential", _.timed.counted.bipartite.withMeasurement("batch-seq"))
        .sequential(
          IO.sleep(1.second),
          IO.sleep(2.seconds),
          IO.raiseError(new Exception),
          IO.sleep(1.seconds))
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }

  test("6.parallel.exception") {
    val jobs = List(
      IO.sleep(1.second),
      IO.sleep(2.seconds),
      IO.sleep(3.seconds),
      IO.sleep(3.seconds) >> IO.raiseError(new Exception),
      IO.sleep(4.seconds))
    service.eventStream { ga =>
      ga.batch("parallel", _.timed.counted.bipartite).parallel(jobs*)
    }.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }
}
