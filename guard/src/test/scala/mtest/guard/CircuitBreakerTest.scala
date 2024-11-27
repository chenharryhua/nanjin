package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toFunctorFilterOps
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.action.CircuitBreaker
import com.github.chenharryhua.nanjin.guard.event.ServiceStopCause
import com.github.chenharryhua.nanjin.guard.event.eventFilters
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

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

  test("3.race - do not really cancel - feature or bug") {
    val io1 = IO.println("start io-1") *> IO.sleep(3.seconds) *> IO.println("end io-1")
    val io2 = IO.println("start io-2") *> IO.sleep(1.seconds) *> IO.println("end io-2")

    val ss = service
      .eventStream(_.circuitBreaker(identity).use { cb =>
        IO.race(cb(io1), cb(io2)) >> IO.sleep(3.seconds)
      })
      .mapFilter(eventFilters.serviceStop)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(ss.cause == ServiceStopCause.Successfully)
  }
}
