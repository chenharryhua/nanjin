package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.batch.{BatchResultState, BatchResultValue}
import com.github.chenharryhua.nanjin.guard.config.LogFormat.Console_JsonNoSpaces
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.{DurationDouble, DurationInt}

class LightBatchTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("light-batch").service("light-batch").updateConfig(_.withLogFormat(Console_JsonNoSpaces))

  test("1.quasi.sequential") {
    val se = service.eventStream { ga =>
      ga.lightBatch("quasi.sequential")
        .sequential[Unit](
          "a" -> IO.raiseError(new Exception()),
          "b" -> IO.sleep(1.second),
          "c" -> IO.sleep(2.seconds),
          "d" -> IO.raiseError(new Exception()),
          "e" -> IO.sleep(1.seconds),
          "f" -> IO.raiseError(new Exception)
        )
        .withJobRename(_ + ":test")
        .quasiBatch
        .map { qr =>
          assert(qr.jobs.forall(_.job.name.endsWith("test")))
          assert(!qr.jobs.head.done)
          assert(qr.jobs(1).done)
          assert(qr.jobs(2).done)
          assert(!qr.jobs(3).done)
          assert(qr.jobs(4).done)
          assert(!qr.jobs(5).done)
          ()
        }
    }.map(checkJson).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("2.quasi.parallel") {
    val se = service.eventStream { ga =>
      ga.lightBatch("quasi.parallel")
        .parallel(3)(
          "a" -> IO.sleep(3.second),
          "b" -> IO.sleep(2.seconds),
          "c" -> IO.raiseError(new Exception).delayBy(2.seconds),
          "d" -> IO.raiseError(new Exception).delayBy(1.seconds),
          "e" -> IO.sleep(4.seconds)
        )
        .withJobRename("test:" + _)
        .quasiBatch
        .map { qr =>
          assert(qr.jobs.forall(_.job.name.startsWith("test")))
          assert(qr.jobs.head.done)
          assert(qr.jobs(1).done)
          assert(!qr.jobs(2).done)
          assert(!qr.jobs(3).done)
          assert(qr.jobs(4).done)
          ()
        }
    }.map(checkJson).compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("3.sequential.value.exception") {
    val se = service.eventStream { ga =>
      ga.lightBatch("sequential")
        .sequential(
          "a" -> IO.sleep(1.second),
          "b" -> IO.raiseError(new Exception).delayBy(2.seconds),
          "c" -> IO.sleep(1.seconds))
        .batchValue
        .void
    }.map(checkJson).compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 3)
  }

  test("4.parallel.value.exception") {
    val jobs = List(
      "a" -> IO.sleep(1.second),
      "b" -> (IO.sleep(2.seconds) >> IO.raiseError(new Exception)),
      "c" -> IO.sleep(3.seconds)
    )
    val se = service.eventStream { ga =>
      ga.lightBatch("parallel").parallel(3)(jobs*).batchValue.void
    }.map(checkJson).compile.lastOrError.unsafeRunSync()

    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 3)
  }

  test("5. sorted parallel") {
    val jobs: List[(String, IO[Int])] = List(
      "1" -> IO(1).delayBy(3.second),
      "2" -> IO(2).delayBy(2.second),
      "3" -> IO(3).delayBy(2.second),
      "4" -> IO(4).delayBy(1.second),
      "5" -> IO(5).delayBy(0.1.second)
    )
    val se = service.eventStream { agent =>
      agent.lightBatch("sorted.parallel").parallel(jobs*).batchValue.map { case BatchResultValue(br, rst) =>
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
        ()
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("predicate.parallel") {
    val jobs =
      List("a" -> IO(1).delayBy(3.second), "b" -> IO(2).delayBy(2.seconds), "c" -> IO(3).delayBy(1.seconds))
    val se = service.eventStream { agent =>
      agent.lightBatch("predicate.value").parallel(jobs*).withPredicate(_ < 2).quasiBatch.map {
        case BatchResultState(_, _, _, _, jobs) =>
          assert(jobs.head.done)
          assert(!jobs(1).done)
          assert(!jobs(2).done)
          ()
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }
  test("predicate.sequential") {
    val jobs =
      List("a" -> IO(1), "b" -> IO(2), "c" -> IO(3))
    val se = service.eventStream { agent =>
      agent.lightBatch("predicate.value").sequential(jobs*).withPredicate(_ < 2).quasiBatch.map {
        case BatchResultState(_, _, _, _, jobs) =>
          assert(jobs.head.done)
          assert(!jobs(1).done)
          assert(!jobs(2).done)
          ()
      }
    }.compile.lastOrError.unsafeRunSync()
    assert(se.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }
}
