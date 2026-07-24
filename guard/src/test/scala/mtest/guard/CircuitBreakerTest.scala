package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toFunctorFilterOps
import com.github.chenharryhua.nanjin.common.resilience.CircuitBreaker
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.{Event, StopReason}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class CircuitBreakerTest extends AnyFunSuite {
  private val service = TaskGuard[IO]("circuit.breaker").service("circuit.breaker")

  test("1.max failures") {
    val good = IO(1)
    val bad = IO.raiseError[Int](new Exception("bad"))
    val ss = service.eventStream { agent =>
      val circuitBreaker = for {
        cb <- agent.circuitBreaker(maxFailures = 3, _.fixedDelay(1.seconds))
        _ <- agent.facilitate("test")(_.gauge("circuit.breaker", _.register(cb.getState)))
      } yield cb

      circuitBreaker.use { cb =>
        cb.attempt(bad) >> agent.adhoc.report >>
          cb.attempt(bad) >> agent.adhoc.report >>
          cb.protect(good) >> agent.adhoc.report.void
      }
    }.mapFilter(Event.serviceStop.getOption).compile.lastOrError.unsafeRunSync()
    assert(ss.cause == StopReason.Successfully)
  }

  test("2.max failures - exceeds") {
    val good = IO(1)
    val bad = IO.raiseError[Int](new Exception("bad"))
    val ss = service.eventStream { agent =>
      val circuitBreaker = for {
        cb <- agent.circuitBreaker(maxFailures = 3, _.fixedDelay(1.seconds))
        _ <- agent.facilitate("test")(_.gauge("circuit.breaker", _.register(cb.getState)))
      } yield cb

      circuitBreaker.use { cb =>
        cb.attempt(bad) >> agent.adhoc.report >>
          cb.attempt(bad) >> agent.adhoc.report >>
          cb.attempt(bad) >> agent.adhoc.report >>
          cb.attempt(bad) >> agent.adhoc.report >>
          cb.protect(good).guarantee(agent.adhoc.report.void).void
      }
    }.mapFilter(Event.serviceStop.getOption).compile.lastOrError.unsafeRunSync()
    assert(
      ss.cause
        .asInstanceOf[StopReason.ByException]
        .stackTrace
        .value
        .head
        .contains("CircuitBreaker rejected"))
  }

  test("3.race") {
    var i = 0
    var j = 0
    val io1 = IO.println("start io-1") *> IO.sleep(3.seconds) *> IO { i = 1 } *> IO.println("end io-1")
    val io2 = IO.println("start io-2") *> IO.sleep(1.seconds) *> IO { j = 1 } *> IO.println("end io-2")

    val ss = service
      .eventStream(_.circuitBreaker(maxFailures = 5, _.fixedDelay(1.seconds)).use { cb =>
        IO.race(cb.protect(io1), cb.protect(io2)) >> IO.sleep(4.seconds)
      })
      .mapFilter(Event.serviceStop.getOption)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(ss.cause == StopReason.Successfully)
    assert(i == 0)
    assert(j == 1)
  }

  test("4.non-positive maxFailures fails configuration") {
    val ss = service
      .eventStream(_.circuitBreaker(maxFailures = 0, _.empty).use { cb =>
        cb.protect(IO.raiseError(new Exception())).attempt >>
          cb.protect(IO(1)).void
      })
      .mapFilter(Event.serviceStop.getOption)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(ss.cause.exitCode == 3)
  }

  test("5.reset state") {
    val ss = service
      .eventStream(_.circuitBreaker(maxFailures = 1, f = _.fixedRate(1.seconds)).use { cb =>
        cb.protect(IO.raiseError(new Exception())).attempt >>
          IO.sleep(2.seconds) >>
          cb.protect(IO(1)).void
      })
      .mapFilter(Event.serviceStop.getOption)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(ss.cause.exitCode == 0)
  }

}
