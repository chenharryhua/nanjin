package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard._
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

import scala.concurrent.duration._

final case class MyException() extends Exception("my exception")

class RetryTest extends AnyFunSuite {

  val serviceGuard = TaskGuard[IO]("retry-guard")
    .service("retry-test")
    .updateConfig(_.health_check_interval(3.hours).constant_delay(1.seconds))

  val logging =
    SlackService(SimpleNotificationService.fake[IO]) |+| MetricsService[IO](new MetricRegistry()) |+| LogService[IO]

  test("success") {
    var i = 0
    val Vector(a, b, c) = serviceGuard.eventStream { gd =>
      gd("succ")
        .updateConfig(_.max_retries(3).full_jitter_backoff(1.second))
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

  test("should retry 2 times when operation fail") {
    var i = 0
    val Vector(a, b, c, d, e) = serviceGuard.eventStream { gd =>
      gd("1-time-succ")("2-time-succ") // funny syntax
        .updateConfig(_.max_retries(3).full_jitter_backoff(1.second).slack_none.slack_first_fail_on)
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

  test("should escalate to up level if retry failed") {
    val Vector(a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.constant_delay(1.hour))
      .eventStream { gd =>
        gd("escalate-after-3-time")
          .updateConfig(_.max_retries(3).fibonacci_backoff(0.1.second))
          .retry(1)(x => IO.raiseError[Int](new Exception("oops")))
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

  test("Null pointer exception") {
    val a :: b :: c :: d :: e :: rest = serviceGuard
      .updateConfig(_.constant_delay(1.hour))
      .eventStream(ag =>
        ag("null exception")
          .updateConfig(_.constant_delay(1.second).max_retries(2))
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

  test("predicate - should retry") {
    val Vector(a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.constant_delay(1.hour))
      .eventStream { gd =>
        gd("predicate")
          .updateConfig(_.max_retries(3).fibonacci_backoff(0.1.second))
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

  test("predicate - should not retry") {
    val Vector(a, b, c) = serviceGuard
      .updateConfig(_.constant_delay(1.hour))
      .eventStream { gd =>
        gd("predicate")
          .updateConfig(_.max_retries(3).fibonacci_backoff(0.1.second))
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

  test("should fail the action if post condition is unsatisfied") {
    val Vector(a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.constant_delay(1.hour))
      .eventStream { gd =>
        gd("postCondition")
          .updateConfig(_.constant_delay(1.seconds).max_retries(3))
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
  test("should fail the action if post condition is unsatisfied - 2") {
    val Vector(a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.constant_delay(1.hour))
      .eventStream { gd =>
        gd("postCondition")
          .updateConfig(_.constant_delay(1.seconds).max_retries(3))
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

  test("nonterminating - should retry") {
    val Vector(a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.constant_delay(1.hour))
      .eventStream { gd =>
        gd("nonterminating")
          .updateConfig(_.max_retries(3).fibonacci_backoff(0.1.second).non_terminating)
          .retry(IO(1))
          .run
      }
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionRetrying].error.throwable.isInstanceOf[ActionException.UnexpectedlyTerminated])
    assert(c.asInstanceOf[ActionRetrying].error.throwable.isInstanceOf[ActionException.UnexpectedlyTerminated])
    assert(d.asInstanceOf[ActionRetrying].error.throwable.isInstanceOf[ActionException.UnexpectedlyTerminated])
    assert(e.asInstanceOf[ActionRetrying].error.throwable.isInstanceOf[ActionException.UnexpectedlyTerminated])
    assert(f.isInstanceOf[ServicePanic])
  }
}
