package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toFunctorFilterOps
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.action.CircuitBreaker
import com.github.chenharryhua.nanjin.guard.event.ServiceStopCause
import com.github.chenharryhua.nanjin.guard.event.eventFilters
import org.scalatest.funsuite.AnyFunSuite

class CircuitBreakerTest extends AnyFunSuite {
  private val service = TaskGuard[IO]("circuit.breaker").service("circuit.breaker")

  test("1.max failures") {
    val good = IO(1)
    val bad  = IO.raiseError[Int](new Exception("bad"))
    val ss = service
      .eventStream(_.circuitBreaker(_.withMaxFailures(3)).use { cb =>
        cb(bad).attempt >>
          cb(bad).attempt >>
          cb(good).void
      })
      .mapFilter(eventFilters.serviceStop)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(ss.cause == ServiceStopCause.Successfully)
  }

  test("2.max failures - exceeds") {
    val good = IO(1)
    val bad  = IO.raiseError[Int](new Exception("bad"))
    val ss = service
      .eventStream(_.circuitBreaker(_.withMaxFailures(3)).use { cb =>
        cb(bad).attempt >>
          cb(bad).attempt >>
          cb(bad).attempt >>
          cb(good).void
      })
      .mapFilter(eventFilters.serviceStop)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(
      ss.cause
        .asInstanceOf[ServiceStopCause.ByException]
        .error
        .message
        .contains(CircuitBreaker.RejectedException.productPrefix))
  }
}
