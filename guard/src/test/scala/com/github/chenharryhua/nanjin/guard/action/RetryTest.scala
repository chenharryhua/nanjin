package com.github.chenharryhua.nanjin.guard.action

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick, TickStatus}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.DurationInt

class RetryTest extends AnyFunSuite {
  private val state  = TickStatus(Tick.zeroth(UUID.randomUUID(), sydneyTime, Instant.now()))
  private val policy = Policy.fixedDelay(1.second).limited(3)

  test("1.apply - F[A]") {
    val retry = new NJRetry.Impl[IO](state.renewPolicy(policy))
    var i     = 0
    val res   = retry(IO(i += 1) >> IO.raiseError[Int](new Exception))

    assertThrows[Exception](res.unsafeRunSync())
    assert(i == 4)
  }

  test("2.apply - (F[A], Throwable=>Boolean)") {
    val retry = new NJRetry.Impl[IO](state.renewPolicy(policy))
    var i     = 0
    val res   = retry(IO(i += 1) >> IO.raiseError[Int](new Exception), (_: Throwable) => true)

    assertThrows[Exception](res.unsafeRunSync())
    assert(i == 4)
  }

  test("3.apply - (Tick=>F[A])") {
    val retry           = new NJRetry.Impl[IO](state.renewPolicy(policy))
    var lst: List[Long] = List.empty

    val res = retry(t => IO { lst = lst.appended(t.index) } >> IO.raiseError[Int](new Exception))

    assertThrows[Exception](res.unsafeRunSync())
    assert(lst == List(0, 1, 2, 3))
  }

  test("4.apply - (Tick=>F[A], Throwable=>Boolean)") {
    val retry           = new NJRetry.Impl[IO](state.renewPolicy(policy))
    var lst: List[Long] = List.empty

    val res = retry(t => IO { lst = lst.appended(t.index) } >> IO.raiseError[Int](new Exception), _ => true)

    assertThrows[Exception](res.unsafeRunSync())
    assert(lst == List(0, 1, 2, 3))
  }

  test("5.unworthy") {
    val retry           = new NJRetry.Impl[IO](state.renewPolicy(policy))
    var lst: List[Long] = List.empty

    val res = retry(t => IO { lst = lst.appended(t.index) } >> IO.raiseError[Int](new Exception), _ => false)

    assertThrows[Exception](res.unsafeRunSync())
    assert(lst == List(0))
  }

  test("6.giveUp") {
    var i     = 0
    val retry = new NJRetry.Impl[IO](state.renewPolicy(Policy.giveUp))
    val res   = retry(IO(i += 1) >> IO.raiseError[Int](new Exception))

    assertThrows[Exception](res.unsafeRunSync())
    assert(i == 1)
  }

  test("7.nothing wrong") {
    val retry           = new NJRetry.Impl[IO](state.renewPolicy(policy))
    var lst: List[Long] = List.empty

    retry(t => IO { lst = lst.appended(t.index) }).unsafeRunSync()

    assert(lst == List(0))
  }

  test("8.escape retry") {
    object EscapeException extends Exception
    val retry           = new NJRetry.Impl[IO](state.renewPolicy(policy))
    var lst: List[Long] = List.empty

    val res = retry(
      tick => {
        val calc =
          if (tick.index < 2) IO.raiseError[Int](new Exception) else IO.raiseError[Int](EscapeException)
        IO { lst = lst.appended(tick.index) } >> calc
      },
      {
        case EscapeException => false
        case _               => true
      }
    )

    assertThrows[Exception](res.unsafeRunSync())
    assert(lst == List(0, 1, 2))
  }

  test("9. success after retry") {
    val retry = new NJRetry.Impl[IO](state.renewPolicy(policy))
    var i     = 0
    retry { tick =>
      val calc = if (tick.index < 2) IO.raiseError[Int](new Exception) else IO(0)
      IO(i += 1) >> calc
    }.unsafeRunSync()

    assert(i == 3)
  }
}
