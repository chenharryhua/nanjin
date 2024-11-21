package com.github.chenharryhua.nanjin.guard.action

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick, TickStatus}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.DurationInt

class RetryTest extends AnyFunSuite {
  private val state  = TickStatus(Tick.zeroth(UUID.randomUUID(), sydneyTime, Instant.now()))
  private val policy = Policy.fixedDelay(1.second).limited(3)

  test("1.apply - F[A]") {
    val retry = new Retry.Impl[IO](state.renewPolicy(policy))
    var i     = 0
    val res   = retry(IO(i += 1) >> IO.raiseError[Int](new Exception))

    assertThrows[Exception](res.unsafeRunSync())
    assert(i == 4)
  }

  test("2.giveUp") {
    var i     = 0
    val retry = new Retry.Impl[IO](state.renewPolicy(Policy.giveUp))
    val res   = retry(IO(i += 1) >> IO.raiseError[Int](new Exception))

    assertThrows[Exception](res.unsafeRunSync())
    assert(i == 1)
  }

  test("3.conditional retry") {
    val retry                = new Retry.Impl[IO](state.renewPolicy(policy))
    var lst: List[Throwable] = List.empty
    object MyException extends Exception
    object MyException2 extends Exception
    object MyException3 extends Exception
    var i = 0
    val action: IO[Int] = IO(if (i == 0) {
      i += 1
      IO.raiseError(MyException)
    } else if (i == 1) {
      i += 1
      IO.raiseError(MyException2)
    } else if (i == 2) {
      i += 1
      IO.raiseError(MyException3)
    } else {
      IO(0)
    }).flatten

    val res: IO[Int] = retry(
      action,
      (_, ex) => {
        lst = ex :: lst
        if (i < 2) IO(true) else IO(false)
      })

    assertThrows[MyException2.type](res.unsafeRunSync())
    assert(lst == List(MyException2, MyException))

  }

  test("4.performance") {
    var i: Int  = 0
    val timeout = 5.seconds
    TaskGuard[IO]("performance")
      .service("performance")
      .eventStream { agent =>
        agent.createRetry(_.giveUp).use(_.apply(IO(i += 1)).foreverM.timeout(timeout).attempt).void
      }
      .compile
      .drain
      .unsafeRunSync()
    val res = i / timeout.toSeconds
    println(s"performance: $res calls/second")
    assert(res > 2_000_000)
  }
}
