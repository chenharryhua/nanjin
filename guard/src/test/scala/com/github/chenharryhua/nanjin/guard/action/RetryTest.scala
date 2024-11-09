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

  test("2.apply - (Tick=>F[A])") {
    val retry           = new Retry.Impl[IO](state.renewPolicy(policy))
    var lst: List[Long] = List.empty

    val res = retry(t => IO { lst = lst.appended(t.index) } >> IO.raiseError[Int](new Exception))

    assertThrows[Exception](res.unsafeRunSync())
    assert(lst == List(0, 1, 2, 3))
  }

  test("3.apply - full") {
    val retry           = new Retry.Impl[IO](state.renewPolicy(policy))
    var lst: List[Long] = List.empty
    object MyException extends Exception

    val res = retry { (t: Tick, ex: Option[Throwable]) =>
      ex match {
        case Some(_) => IO { lst = lst.appended(t.index) } *> IO(Left(MyException))
        case None    => IO { lst = lst.appended(t.index) } *> IO.raiseError(new Exception)
      }
    }

    assertThrows[MyException.type](res.unsafeRunSync())
    assert(lst == List(0, 1))
  }

  test("4.giveUp") {
    var i     = 0
    val retry = new Retry.Impl[IO](state.renewPolicy(Policy.giveUp))
    val res   = retry(IO(i += 1) >> IO.raiseError[Int](new Exception))

    assertThrows[Exception](res.unsafeRunSync())
    assert(i == 1)
  }

  test("5.nothing wrong") {
    val retry           = new Retry.Impl[IO](state.renewPolicy(policy))
    var lst: List[Long] = List.empty

    retry(t => IO { lst = lst.appended(t.index) }).unsafeRunSync()

    assert(lst == List(0))
  }

  test("6.escape retry") {
    val retry           = new Retry.Impl[IO](state.renewPolicy(policy))
    var lst: List[Long] = List.empty

    val res = retry { (t, ex) =>
      ex match {
        case Some(_) => IO { lst = lst.appended(t.index) } *> IO(Right(0))
        case None    => IO { lst = lst.appended(t.index) } *> IO.raiseError(new Exception)
      }
    }

    assert(res.unsafeRunSync() == 0)
    assert(lst == List(0, 1))

  }

  test("7.conditional retry") {
    val retry                = new Retry.Impl[IO](state.renewPolicy(policy))
    var lst: List[Throwable] = List.empty

    object MyException extends Exception
    object MyException2 extends Exception
    object MyException3 extends Exception

    val res: IO[Int] = retry { (_, oex) =>
      oex match {
        case None => IO.raiseError(MyException)
        case Some(ex) if ex.isInstanceOf[MyException.type] =>
          IO { lst = lst.appended(ex) } *> IO(Left(MyException2))
        case Some(ex) => IO { lst = lst.appended(ex) } *> IO.raiseError(MyException3)
      }
    }

    assertThrows[MyException2.type](res.unsafeRunSync())
    assert(lst == List(MyException))

  }

  test("8.success after retry") {
    val retry = new Retry.Impl[IO](state.renewPolicy(policy))
    var i     = 0
    retry { tick =>
      val calc = if (tick.index < 2) IO.raiseError[Int](new Exception) else IO(0)
      IO(i += 1) >> calc
    }.unsafeRunSync()

    assert(i == 3)
  }

  test("9.performance") {
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
