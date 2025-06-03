package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toFunctorFilterOps
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.action.CircuitBreaker
import com.github.chenharryhua.nanjin.guard.event.ServiceStopCause
import com.github.chenharryhua.nanjin.guard.event.eventFilters
import com.github.chenharryhua.nanjin.guard.observers.console
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class CircuitBreakerTest extends AnyFunSuite {
  private val service = TaskGuard[IO]("circuit.breaker").service("circuit.breaker")

  test("1.max failures") {
    val good = IO(1)
    val bad = IO.raiseError[Int](new Exception("bad"))
    val ss = service.eventStream { agent =>
      val circuitBreaker = for {
        cb <- agent.circuitBreaker(_.withMaxFailures(3))
        _ <- agent.facilitate("test")(_.gauge("circuit.breaker").register(cb.getState))
      } yield cb

      circuitBreaker.use { cb =>
        cb.attempt(bad) >> agent.adhoc.report >>
          cb.attempt(bad) >> agent.adhoc.report >>
          cb.protect(good) >> agent.adhoc.report
      }
    }.evalTap(console.text[IO]).mapFilter(eventFilters.serviceStop).compile.lastOrError.unsafeRunSync()
    assert(ss.cause == ServiceStopCause.Successfully)
  }

  test("2.max failures - exceeds") {
    val good = IO(1)
    val bad = IO.raiseError[Int](new Exception("bad"))
    val ss = service.eventStream { agent =>
      val circuitBreaker = for {
        cb <- agent.circuitBreaker(_.withMaxFailures(3))
        _ <- agent.facilitate("test")(_.gauge("circuit.breaker").register(cb.getState))
      } yield cb

      circuitBreaker.use { cb =>
        cb.attempt(bad) >> agent.adhoc.report >>
          cb.attempt(bad) >> agent.adhoc.report >>
          cb.attempt(bad) >> agent.adhoc.report >>
          cb.protect(good).guarantee(agent.adhoc.report).void
      }
    }.evalTap(console.text[IO]).mapFilter(eventFilters.serviceStop).compile.lastOrError.unsafeRunSync()
    assert(
      ss.cause
        .asInstanceOf[ServiceStopCause.ByException]
        .error
        .message
        .contains(CircuitBreaker.RejectedException.productPrefix))
  }

  test("3.race") {
    var i = 0
    var j = 0
    val io1 = IO.println("start io-1") *> IO.sleep(3.seconds) *> IO { i = 1 } *> IO.println("end io-1")
    val io2 = IO.println("start io-2") *> IO.sleep(1.seconds) *> IO { j = 1 } *> IO.println("end io-2")

    val ss = service
      .eventStream(_.circuitBreaker(identity).use { cb =>
        IO.race(cb.protect(io1), cb.protect(io2)) >> IO.sleep(4.seconds)
      })
      .mapFilter(eventFilters.serviceStop)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(ss.cause == ServiceStopCause.Successfully)
    assert(i == 0)
    assert(j == 1)
  }

  test("4.max failures") {
    val ss = service
      .eventStream(_.circuitBreaker(_.withMaxFailures(0)).use { cb =>
        cb.protect(IO.raiseError(new Exception())).attempt >>
          cb.protect(IO(1)).void
      })
      .mapFilter(eventFilters.serviceStop)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(ss.cause.exitCode == 3)
  }

  test("5.reset state") {
    val ss = service
      .eventStream(_.circuitBreaker(_.withMaxFailures(0).withPolicy(_.fixedRate(1.seconds))).use { cb =>
        cb.protect(IO.raiseError(new Exception())).attempt >>
          IO.sleep(2.seconds) >>
          cb.protect(IO(1)).void
      })
      .mapFilter(eventFilters.serviceStop)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(ss.cause.exitCode == 0)
  }
}
