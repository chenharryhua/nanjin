package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import cats.syntax.all.*

final case class MyException() extends Exception("my exception")

class RetryTest extends AnyFunSuite {

  val serviceGuard: ServiceGuard[IO] =
    TaskGuard[IO]("retry-guard").service("retry-test").updateConfig(_.withConstantDelay(1.seconds))

  test("retry - success trivial") {
    val Vector(s, c) = serviceGuard.eventStream { gd =>
      gd("succ-trivial").trivial
        .updateConfig(_.withMaxRetries(3).withFullJitterBackoff(1.second))
        .retry((x: Int) => IO(x + 1))
        .withSuccNotes((a, b) => s"$a -> $b")
        .withFailNotes((a, e) => s"$a $e")
        .withWorthRetry(_ => true)
        .run(1)
    }.compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("retry - success notice") {
    val Vector(s, a, b, c, d, e, f, g) = serviceGuard.eventStream { gd =>
      val ag = gd("all-succ").notice
        .updateConfig(_.withMaxRetries(3).withFullJitterBackoff(1.second))
        .retry((x: Int) => IO(x + 1))
        .withSuccNotes((a, b) => s"$a->$b")
        .withFailNotes((a, e) => "")
        .withWorthRetry(_ => true)
      List(1, 2, 3).traverse(i => ag.run(i))
    }.compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionSucced].notes.value == "1->2")
    assert(c.isInstanceOf[ActionStart])
    assert(d.asInstanceOf[ActionSucced].notes.value == "2->3")
    assert(e.isInstanceOf[ActionStart])
    assert(f.asInstanceOf[ActionSucced].notes.value == "3->4")
    assert(g.isInstanceOf[ServiceStopped])
  }

  test("retry - should retry 2 times when operation fail") {
    var i = 0
    val Vector(s, a, b, c, d, e) = serviceGuard.eventStream { gd =>
      gd("1-time-succ")("2-time-succ").notice // funny syntax
        .updateConfig(_.withMaxRetries(3).withFullJitterBackoff(1.second))
        .retry((x: Int) =>
          IO(if (i < 2) {
            i += 1; throw new Exception
          } else i))
        .run(1)
    }.compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.asInstanceOf[ActionSucced].numRetries == 2)
    assert(e.isInstanceOf[ServiceStopped])
  }

  test("retry - should retry 2 times when operation fail - low") {
    var i = 0
    val Vector(s, b, c, e) = serviceGuard.eventStream { gd =>
      gd("1-time-succ").trivial
        .updateConfig(_.withMaxRetries(3).withFullJitterBackoff(1.second))
        .retry((x: Int) =>
          IO(if (i < 2) {
            i += 1; throw new Exception
          } else i))
        .run(1)
    }.compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(e.isInstanceOf[ServiceStopped])
  }

  test("retry - should escalate to up level if retry failed") {
    val Vector(s, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd("escalate-after-3-times").trivial
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .retry((x: Int) => IO.raiseError[Int](new Exception("oops")))
          .run(1)
      }
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionRetrying])
    assert(e.isInstanceOf[ActionFailed])
    assert(f.isInstanceOf[ServicePanic])
  }

  test("retry - Null pointer exception") {
    val s :: b :: c :: d :: e :: rest = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream(ag =>
        ag("null exception")
          .updateConfig(_.withCapDelay(1.second).withMaxRetries(2))
          .retry(IO.raiseError(new NullPointerException))
          .run)
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.asInstanceOf[ActionFailed].numRetries == 2)
    assert(e.isInstanceOf[ServicePanic])
  }

  test("retry - predicate - should retry") {
    val Vector(s, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd("predicate").trivial
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .retry(IO.raiseError(MyException()))
          .withWorthRetry(ex => (ex.isInstanceOf[MyException]))
          .run
      }
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionRetrying])
    assert(e.isInstanceOf[ActionFailed])
    assert(f.isInstanceOf[ServicePanic])
  }

  test("retry - predicate - should not retry") {
    val Vector(s, a, b, c) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd("predicate").notice
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .retry(IO.raiseError(new Exception()))
          .withWorthRetry(_.isInstanceOf[MyException])
          .run
      }
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionFailed].numRetries == 0)
    assert(c.isInstanceOf[ServicePanic])
  }

  test("retry - should fail the action if post condition is unsatisfied") {
    val Vector(s, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd("postCondition").trivial
          .updateConfig(_.withConstantDelay(1.seconds).withMaxRetries(3))
          .retry(IO(0))
          .withPostCondition(_ > 1)
          .run
      }
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionRetrying])
    assert(e.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action post condition unsatisfied")
    assert(f.isInstanceOf[ServicePanic])
  }
  test("retry - should fail the action if post condition is unsatisfied - 2") {
    val Vector(s, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd("postCondition").trivial
          .updateConfig(_.withConstantDelay(1.seconds).withMaxRetries(3))
          .retry((a: Int) => IO(a))
          .withPostCondition(_ > 1)
          .run(0)
      }
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionRetrying])
    assert(e.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action post condition unsatisfied")
    assert(f.isInstanceOf[ServicePanic])
  }

  test("retry - nonterminating - should retry") {
    val s :: b :: c :: s1 :: e :: f :: s2 :: h :: i :: rest = serviceGuard
      .updateConfig(_.withConstantDelay(1.second))
      .eventStream(_.nonStop(fs2.Stream(1))) // suppose run forever but...
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(b.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action was terminated unexpectedly")
    assert(c.isInstanceOf[ServicePanic])
    assert(s1.isInstanceOf[ServiceStarted])
    assert(e.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action was terminated unexpectedly")
    assert(f.isInstanceOf[ServicePanic])
    assert(s2.isInstanceOf[ServiceStarted])
    assert(h.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action was terminated unexpectedly")
    assert(i.isInstanceOf[ServicePanic])
  }

  test("retry - nonterminating - exception") {

    val s :: b :: c :: s1 :: e :: f :: s2 :: h :: i :: rest = serviceGuard
      .updateConfig(_.withConstantDelay(1.second))
      .eventStream(_.nonStop(IO.raiseError(new Exception("ex"))))
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(b.asInstanceOf[ActionFailed].error.throwable.get.asInstanceOf[Exception].getMessage == "ex")
    assert(c.isInstanceOf[ServicePanic])
    assert(s1.isInstanceOf[ServiceStarted])
    assert(e.asInstanceOf[ActionFailed].error.throwable.get.asInstanceOf[Exception].getMessage == "ex")
    assert(f.isInstanceOf[ServicePanic])
    assert(s2.isInstanceOf[ServiceStarted])
    assert(h.asInstanceOf[ActionFailed].error.throwable.get.asInstanceOf[Exception].getMessage == "ex")
    assert(i.isInstanceOf[ServicePanic])
  }

  test("retry - nonterminating - cancelation") {
    val s :: b :: c :: s2 :: e :: f :: s3 :: h :: i :: rest = serviceGuard
      .updateConfig(_.withConstantDelay(1.second))
      .eventStream(_.nonStop(IO(1) >> IO.canceled))
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(b.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action was canceled internally")
    assert(c.isInstanceOf[ServicePanic])
    assert(s2.isInstanceOf[ServiceStarted])
    assert(e.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action was canceled internally")
    assert(f.isInstanceOf[ServicePanic])
    assert(s3.isInstanceOf[ServiceStarted])
    assert(h.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action was canceled internally")
    assert(i.isInstanceOf[ServicePanic])
  }

}
