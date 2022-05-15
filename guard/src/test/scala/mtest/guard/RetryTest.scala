package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.action.NJRetry
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import eu.timepit.refined.auto.*

final case class MyException() extends Exception("my exception")

class RetryTest extends AnyFunSuite {

  val serviceGuard: ServiceGuard[IO] =
    TaskGuard[IO]("retry-guard").service("retry test").updateConfig(_.withConstantDelay(1.seconds))

  test("1.retry - success trivial") {
    val Vector(s, c) = serviceGuard.eventStream { gd =>
      gd.span("succ-trivial")
        .updateConfig(_.withMaxRetries(3).withFullJitterBackoff(1.second))
        .retry((x: Int) => IO(x + 1))
        .withSuccNotes((a, b) => s"$a -> $b")
        .withFailNotes((a, e) => s"$a $e")
        .withWorthRetry(_ => true)
        .run(1)
    }.compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(c.isInstanceOf[ServiceStop])
  }

  test("2.retry - success notice") {
    val Vector(s, a, b, c, d, e, f, g) = serviceGuard.eventStream { gd =>
      val ag = gd
        .span("all-succ")
        .notice
        .updateConfig(_.withMaxRetries(3).withFullJitterBackoff(1.second))
        .retry((x: Int) => IO(x + 1))
        .withSuccNotes((a, b) => s"$a->$b")
        .withFailNotes((a, e) => "")
        .withWorthRetry(_ => true)
      List(1, 2, 3).traverse(i => ag.run(i))
    }.compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionSucc].notes.value == "1->2")
    assert(c.isInstanceOf[ActionStart])
    assert(d.asInstanceOf[ActionSucc].notes.value == "2->3")
    assert(e.isInstanceOf[ActionStart])
    assert(f.asInstanceOf[ActionSucc].notes.value == "3->4")
    assert(g.isInstanceOf[ServiceStop])
  }

  test("3.retry - all fail") {
    val Vector(s, a, b, c, d, e, f, g, h, i, j) = serviceGuard.eventStream { gd =>
      val ag: NJRetry[IO, Int, Int] = gd
        .span("all-fail")
        .notice
        .updateConfig(_.withMaxRetries(1).withConstantDelay(0.1.second))
        .retry((x: Int) => IO.raiseError[Int](new Exception))
        .withFailNotes((a, e) => a.toString)
      List(1, 2, 3).traverse(i => ag.run(i).attempt)
    }.compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.asInstanceOf[ActionFail].notes.value == "1")
    assert(d.isInstanceOf[ActionStart])
    assert(e.isInstanceOf[ActionRetry])
    assert(f.asInstanceOf[ActionFail].notes.value == "2")
    assert(g.isInstanceOf[ActionStart])
    assert(h.isInstanceOf[ActionRetry])
    assert(i.asInstanceOf[ActionFail].notes.value == "3")
    assert(j.isInstanceOf[ServiceStop])
  }

  test("4.retry - should retry 2 times when operation fail") {
    var i = 0
    val Vector(s, a, b, c, d, e) = serviceGuard.eventStream { gd =>
      gd.span("1-time-succ")
        .span("2-time-succ")
        .notice // funny syntax
        .updateConfig(_.withMaxRetries(3).withFullJitterBackoff(1.second))
        .retry((x: Int) =>
          IO(if (i < 2) {
            i += 1; throw new Exception
          } else i))
        .run(1)
    }.compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.asInstanceOf[ActionSucc].numRetries == 2)
    assert(e.isInstanceOf[ServiceStop])
  }

  test("5.retry - should retry 2 times when operation fail - low") {
    var i = 0
    val Vector(s, b, c, e) = serviceGuard.eventStream { gd =>
      gd.span("1-time-succ")
        .updateConfig(_.withMaxRetries(3).withFullJitterBackoff(1.second))
        .retry((x: Int) =>
          IO(if (i < 2) {
            i += 1; throw new Exception
          } else i))
        .run(1)
    }.compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ServiceStop])
  }

  test("6.retry - should escalate to up level if retry failed") {
    val Vector(s, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd.span("escalate-after-3-times")
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .retry((x: Int) => IO.raiseError[Int](new Exception("oops")))
          .run(1)
      }
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionFail])
    assert(f.isInstanceOf[ServicePanic])
  }

  test("7.retry - Null pointer exception") {
    val s :: b :: c :: d :: e :: rest = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream(ag =>
        ag.span("null exception")
          .updateConfig(_.withCapDelay(1.second).withMaxRetries(2))
          .retry(IO.raiseError(new NullPointerException))
          .run)
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.asInstanceOf[ActionFail].numRetries == 2)
    assert(e.isInstanceOf[ServicePanic])
  }

  test("8.retry - predicate - should retry") {
    val Vector(s, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd.span("predicate")
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .retry(IO.raiseError(MyException()))
          .withWorthRetry(_.isInstanceOf[MyException])
          .run
      }
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionFail])
    assert(f.isInstanceOf[ServicePanic])
  }

  test("9.retry - isWorthRetry - should not retry") {
    val Vector(s, a, b, c) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd.span("predicate")
          .notice
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .retry(IO.raiseError(new Exception))
          .withWorthRetry(_.isInstanceOf[MyException])
          .run
      }
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionFail].numRetries == 0)
    assert(c.isInstanceOf[ServicePanic])
  }

  test("12.retry - nonterminating - should retry") {
    val a :: b :: c :: d :: e :: f :: rest = serviceGuard
      .updateConfig(_.withConstantDelay(1.second))
      .eventStream(_.nonStop(fs2.Stream(1))) // suppose run forever but...
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()

    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServicePanic])
    assert(c.isInstanceOf[ServiceStart])
    assert(d.isInstanceOf[ServicePanic])
    assert(e.isInstanceOf[ServiceStart])
    assert(f.isInstanceOf[ServicePanic])
  }

  test("13.retry - nonterminating - exception") {

    val a :: b :: c :: d :: e :: f :: g :: h :: i :: rest = serviceGuard
      .updateConfig(_.withConstantDelay(1.second))
      .eventStream(_.nonStop(IO.raiseError(new Exception("ex"))))
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()

    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[ActionFail].error.message == "Exception: ex")
    assert(c.isInstanceOf[ServicePanic])
    assert(d.isInstanceOf[ServiceStart])
    assert(e.asInstanceOf[ActionFail].error.message == "Exception: ex")
    assert(f.isInstanceOf[ServicePanic])
    assert(g.isInstanceOf[ServiceStart])
    assert(h.asInstanceOf[ActionFail].error.message == "Exception: ex")
    assert(i.isInstanceOf[ServicePanic])
  }

  test("14.retry - nonterminating - cancelation") {
    val a :: b :: c :: Nil = serviceGuard
      .updateConfig(_.withConstantDelay(1.second))
      .eventStream(_.nonStop(IO(1) >> IO.canceled))
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionFail])
    assert(c.isInstanceOf[ServiceStop])
  }

  test("15.quasi syntax") {
    serviceGuard.eventStream { ag =>
      ag.quasi(3)(IO("a"), IO("b")) >>
        ag.quasi(3, List(IO("a"), IO("b"))) >>
        ag.quasi(List(IO("a"), IO("b"))) >>
        ag.quasi(IO("a"), IO("b")) >>
        ag.quasi(IO.print("a"), IO.print("b")) >>
        ag.quasi(3)(IO.print("a"), IO.print("b"))
    }
  }
}
