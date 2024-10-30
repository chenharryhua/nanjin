package mtest.guard

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.chrono.zones.singaporeTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

class RetryTest extends AnyFunSuite {

  private val service =
    TaskGuard[IO]("retry").updateConfig(_.withZoneId(singaporeTime)).service("retry")

  private val policy: Policy = Policy.fixedDelay(1.seconds).limited(3)

  test("1.apply - F[A]") {
    var i = 0
    service.eventStream {
      _.facilitate("fa", _.withPolicy(policy)) { fac =>
        Resource.eval {
          fac.retry(IO(i += 1) >> IO.raiseError[Int](new Exception))
        }
      }.use_
    }.compile.drain.unsafeRunSync()
    assert(i == 4)
  }

  test("2.apply - (F[A], Throwable=>Boolean)") {
    var i = 0
    service.eventStream {
      _.facilitate("fa", _.withPolicy(policy)) { fac =>
        Resource.eval {
          fac.retry(IO(i += 1) >> IO.raiseError[Int](new Exception), (_: Throwable) => true)
        }
      }.use_
    }.compile.drain.unsafeRunSync()
    assert(i == 4)
  }

  test("3.apply - (Tick=>F[A])") {
    var lst: List[Long] = List.empty
    service.eventStream {
      _.facilitate("fa", _.withPolicy(policy)) { fac =>
        Resource.eval {
          fac.retry(t => IO { lst = lst.appended(t.index) } >> IO.raiseError[Int](new Exception))
        }
      }.use_
    }.compile.drain.unsafeRunSync()
    assert(lst == List(0, 1, 2, 3))
  }

  test("4.apply - (Tick=>F[A], Throwable=>Boolean)") {
    var lst: List[Long] = List.empty
    service.eventStream {
      _.facilitate("fa", _.withPolicy(policy)) { fac =>
        Resource.eval {
          fac.retry(t => IO { lst = lst.appended(t.index) } >> IO.raiseError[Int](new Exception), _ => true)
        }
      }.use_
    }.compile.drain.unsafeRunSync()
    assert(lst == List(0, 1, 2, 3))
  }

  test("5.unworthy") {
    var lst: List[Long] = List.empty
    service.eventStream {
      _.facilitate("fa", _.withPolicy(policy)) { fac =>
        Resource.eval {
          fac.retry(t => IO { lst = lst.appended(t.index) } >> IO.raiseError[Int](new Exception), _ => false)
        }
      }.use_
    }.compile.drain.unsafeRunSync()
    assert(lst == List(0))
  }

  test("6.giveUp") {
    var i = 0
    service.eventStream {
      _.facilitate("fa", _.withPolicy(Policy.giveUp)) { fac =>
        Resource.eval {
          fac.retry(IO(i += 1) >> IO.raiseError[Int](new Exception))
        }
      }.use_
    }.compile.drain.unsafeRunSync()
    assert(i == 1)
  }

  test("7.nothing wrong") {
    var lst: List[Long] = List.empty
    service.eventStream {
      _.facilitate("fa", _.withPolicy(policy)) { fac =>
        Resource.eval {
          fac.retry(t => IO { lst = lst.appended(t.index) })
        }
      }.use_
    }.compile.drain.unsafeRunSync()
    assert(lst == List(0))
  }

  test("8.escape retry") {
    object EscapeException extends Exception
    var lst: List[Long] = List.empty
    service.eventStream {
      _.facilitate("fa", _.withPolicy(policy)) { fac =>
        Resource.eval {
          fac.retry(
            tick =>
              IO { lst = lst.appended(tick.index) } >> {
                if (tick.index < 2) IO.raiseError[Int](new Exception)
                else IO.raiseError[Int](EscapeException)
              },
            {
              case EscapeException => false
              case _               => true
            }
          )
        }
      }.use_
    }.compile.drain.unsafeRunSync()
    assert(lst == List(0, 1, 2))
  }
}
