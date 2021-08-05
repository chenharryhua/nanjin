package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.action.ActionException
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionFailed,
  ActionRetrying,
  ActionStart,
  ActionSucced,
  LogService,
  MetricsService,
  ServicePanic,
  ServiceStopped,
  SlackService
}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

final case class MyException() extends Exception("my exception")

class RetryTest extends AnyFunSuite {

  val serviceGuard = TaskGuard[IO]("retry-guard")
    .service("retry-test")
    .updateConfig(_.withHealthCheckInterval(3.hours).withConstantDelay(1.seconds))

  val logging =
    MetricsService[IO](new MetricRegistry()) |+| LogService[IO]

  test("retry - success") {
    var i = 0
    val Vector(a, b, c) = serviceGuard.eventStream { gd =>
      gd("succ")
        .updateConfig(_.withMaxRetries(3).withFullJitterBackoff(1.second))
        .retry(1)(x => IO(x + 1))
        .withSuccNotes((a, b) => s"$a -> $b")
        .withFailNotes((a, e) => "")
        .withWorthRetry(_ => true)
        .run
    }.observe(_.evalMap(logging.alert).drain).compile.toVector.unsafeRunSync()
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionSucced])
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("retry - should retry 2 times when operation fail") {
    var i = 0
    val Vector(a, b, c, d, e) = serviceGuard.eventStream { gd =>
      gd("1-time-succ")("2-time-succ") // funny syntax
        .updateConfig(_.withMaxRetries(3).withFullJitterBackoff(1.second).withSlackNone.withSlackFirstFailOn)
        .retry(1)(x =>
          IO(if (i < 2) {
            i += 1; throw new Exception
          } else i))
        .run
    }.observe(_.evalMap(logging.alert).drain).compile.toVector.unsafeRunSync()
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.asInstanceOf[ActionSucced].numRetries == 2)
    assert(e.isInstanceOf[ServiceStopped])
  }

  test("retry - should escalate to up level if retry failed") {
    val Vector(a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd("escalate-after-3-time")
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .retry(1)(x => IO.raiseError[Int](new Exception("oops")))
          .run
      }
      .debug()
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionRetrying])
    assert(e.isInstanceOf[ActionFailed])
    assert(f.isInstanceOf[ServicePanic])
  }

  test("retry - Null pointer exception") {
    val a :: b :: c :: d :: e :: rest = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream(ag =>
        ag("null exception")
          .updateConfig(_.withCapDelay(1.second).withMaxRetries(2))
          .loudly(IO.raiseError(new NullPointerException)))
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.asInstanceOf[ActionFailed].numRetries == 2)
    assert(e.isInstanceOf[ServicePanic])
  }

  test("retry - predicate - should retry") {
    val Vector(a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd("predicate")
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .retry(IO.raiseError(MyException()))
          .withWorthRetry(ex => (ex.isInstanceOf[MyException]))
          .run
      }
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionRetrying])
    assert(e.isInstanceOf[ActionFailed])
    assert(f.isInstanceOf[ServicePanic])
  }

  test("retry - predicate - should not retry") {
    val Vector(a, b, c) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd("predicate")
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .retry(IO.raiseError(new Exception()))
          .withWorthRetry(_.isInstanceOf[MyException])
          .run
      }
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionFailed].numRetries == 0)
    assert(c.isInstanceOf[ServicePanic])
  }

  test("retry - should fail the action if post condition is unsatisfied") {
    val Vector(a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd("postCondition")
          .updateConfig(_.withConstantDelay(1.seconds).withMaxRetries(3))
          .retry(IO(0))
          .withPostCondition(_ > 1)
          .run
      }
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionRetrying])
    assert(e.asInstanceOf[ActionFailed].error.throwable.isInstanceOf[ActionException.PostConditionUnsatisfied])
    assert(f.isInstanceOf[ServicePanic])
  }
  test("retry - should fail the action if post condition is unsatisfied - 2") {
    val Vector(a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd("postCondition")
          .updateConfig(_.withConstantDelay(1.seconds).withMaxRetries(3))
          .retry(0)(IO(_))
          .withPostCondition(_ > 1)
          .run
      }
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionRetrying])
    assert(e.asInstanceOf[ActionFailed].error.throwable.isInstanceOf[ActionException.PostConditionUnsatisfied])
    assert(f.isInstanceOf[ServicePanic])
  }

  test("retry - nonterminating - should retry") {
    val a :: b :: c :: d :: e :: f :: g :: h :: i :: rest = serviceGuard
      .updateConfig(_.withConstantDelay(1.second).withStartupDelay(1.hour))
      .eventStream(_.nonStop(fs2.Stream(1))) // suppose run forever but...
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionFailed].error.throwable.isInstanceOf[ActionException.UnexpectedlyTerminated])
    assert(c.isInstanceOf[ServicePanic])
    assert(d.isInstanceOf[ActionStart])
    assert(e.asInstanceOf[ActionFailed].error.throwable.isInstanceOf[ActionException.UnexpectedlyTerminated])
    assert(f.isInstanceOf[ServicePanic])
    assert(g.isInstanceOf[ActionStart])
    assert(h.asInstanceOf[ActionFailed].error.throwable.isInstanceOf[ActionException.UnexpectedlyTerminated])
    assert(i.isInstanceOf[ServicePanic])
  }

  test("retry - nonterminating - exception") {

    val a :: b :: c :: d :: e :: f :: g :: h :: i :: rest = serviceGuard
      .updateConfig(_.withConstantDelay(1.second).withStartupDelay(1.hour))
      .eventStream(_.nonStop(IO.raiseError(new Exception("ex"))))
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionFailed].error.throwable.asInstanceOf[Exception].getMessage == "ex")
    assert(c.isInstanceOf[ServicePanic])
    assert(d.isInstanceOf[ActionStart])
    assert(e.asInstanceOf[ActionFailed].error.throwable.asInstanceOf[Exception].getMessage == "ex")
    assert(f.isInstanceOf[ServicePanic])
    assert(g.isInstanceOf[ActionStart])
    assert(h.asInstanceOf[ActionFailed].error.throwable.asInstanceOf[Exception].getMessage == "ex")
    assert(i.isInstanceOf[ServicePanic])
  }

  test("retry - nonterminating - cancelation") {
    val a :: b :: c :: d :: e :: f :: g :: h :: i :: rest = serviceGuard
      .updateConfig(_.withConstantDelay(1.second).withStartupDelay(1.hour))
      .eventStream(_.nonStop(IO(1) >> IO.canceled))
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionFailed].error.throwable.isInstanceOf[ActionException.ActionCanceledInternally])
    assert(c.isInstanceOf[ServicePanic])
    assert(d.isInstanceOf[ActionStart])
    assert(e.asInstanceOf[ActionFailed].error.throwable.isInstanceOf[ActionException.ActionCanceledInternally])
    assert(f.isInstanceOf[ServicePanic])
    assert(g.isInstanceOf[ActionStart])
    assert(h.asInstanceOf[ActionFailed].error.throwable.isInstanceOf[ActionException.ActionCanceledInternally])
    assert(i.isInstanceOf[ServicePanic])
  }
}
