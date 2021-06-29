package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard._
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionFailed,
  ActionRetrying,
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
    .updateConfig(_.withHealthCheckInterval(3.hours).withConstantDelay(1.seconds))

  val logging =
    SlackService(SimpleNotificationService.fake[IO]) |+| MetricsService[IO](new MetricRegistry()) |+| LogService[IO]

  test("success") {
    var i = 0
    val Vector(a, b) = serviceGuard
      .updateConfig(_.withNormalStop)
      .eventStream { gd =>
        gd("succ")
          .updateConfig(
            _.withMaxRetries(3).withFullJitter(1.second).withRetryAlertOn.withFYIAlertOff.withFirstFailAlertOn)
          .retry(1)(x => IO(x + 1))
          .withSuccNotes((a, b) => s"$a -> $b")
          .withFailNotes((a, e) => "")
          .withPredicate(_ => IO(true))
          .run
      }
      .observe(_.evalMap(logging.alert).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionSucced])
    assert(b.isInstanceOf[ServiceStopped])
  }

  test("should retry 2 times when operation fail") {
    var i = 0
    val Vector(a, b, c, d) = serviceGuard
      .updateConfig(_.withNormalStop)
      .eventStream { gd =>
        gd("1-time-succ")("2-time-succ") // funny syntax
          .updateConfig(_.withMaxRetries(3).withFullJitter(1.second))
          .retry(1)(x =>
            IO(if (i < 2) {
              i += 1; throw new Exception
            } else i))
          .run
      }
      .observe(_.evalMap(logging.alert).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionRetrying])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.asInstanceOf[ActionSucced].numRetries == 2)
    assert(d.isInstanceOf[ServiceStopped])
  }

  test("should escalate to up level if retry failed") {
    val Vector(a, b, c, d, e) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd("escalate-after-3-time")
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .retry(1)(x => IO.raiseError[Int](new Exception("oops")))
          .run
      }
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionRetrying])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionFailed])
    assert(e.isInstanceOf[ServicePanic])
  }

  test("Null pointer exception") {
    val a :: b :: c :: d :: rest = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream(ag =>
        ag("null exception")
          .updateConfig(_.withConstantDelay(1.second).withMaxRetries(2))
          .loudly(IO.raiseError(new NullPointerException)))
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionRetrying])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.asInstanceOf[ActionFailed].numRetries == 2)
    assert(d.isInstanceOf[ServicePanic])
  }

  test("predicate - should retry") {
    val Vector(a, b, c, d, e) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd("predicate")
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .retry(IO.raiseError(MyException()))
          .withPredicate(ex => IO(ex.isInstanceOf[MyException]))
          .run
      }
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionRetrying])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionFailed])
    assert(e.isInstanceOf[ServicePanic])
  }

  test("predicate - should not retry") {
    val Vector(a, b) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd("predicate")
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .retry(IO.raiseError(new Exception()))
          .withPredicate(ex => IO(ex.isInstanceOf[MyException]))
          .run
      }
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.asInstanceOf[ActionFailed].numRetries == 0)
    assert(b.isInstanceOf[ServicePanic])
  }
}
