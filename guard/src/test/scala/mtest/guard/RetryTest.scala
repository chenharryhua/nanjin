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
class RetryTest extends AnyFunSuite {

  val serviceGuard = TaskGuard[IO]("retry-guard")
    .service("retry-test")
    .updateConfig(_.withHealthCheckInterval(3.hours).withConstantDelay(1.seconds))

  val logging =
    SlackService(SimpleNotificationService.fake[IO]) |+| MetricsService[IO](new MetricRegistry()) |+| LogService[IO]

  test("should retry 2 times when operation fail") {
    var i = 0
    val Vector(a, b, c, d) = serviceGuard
      .updateConfig(_.withNormalStop)
      .eventStream { gd =>
        gd("1-time-succ")("2-time-succ") // funny syntax
          .updateConfig(_.withMaxRetries(3).withFullJitter(1.second))
          .run(IO(if (i < 2) {
            i += 1; throw new Exception
          } else i))
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
          .retry(IO.raiseError(new Exception("oops")))
          .withSuccNotes((_, _: Int) => "")
          .withFailNotes((_, _) => "")
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

  test("cancellation") {
    var i: Int = 0
    val Vector(a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { action =>
        val a1 = action("succ-1").run(IO.sleep(1.second) >> IO(1))
        val a2 = action("cancel-2")
          .updateConfig(_.withConstantDelay(1.second).withMaxRetries(100))
          .run(IO(if (i < 1) { i += 1; throw new Exception }
          else 1) >> IO.never[Int])
        val a3 = action("cancel-3").run(IO.never[Int])
        action("supervisor").run(IO.parSequenceN(5)(List(IO.sleep(3.second) >> IO.canceled, a1, a2, a3)).void)
      }
      .observe(_.evalMap(logging.alert).drain)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionRetrying]) // a2
    assert(b.isInstanceOf[ActionSucced]) // a1
    assert(c.isInstanceOf[ActionFailed])
    assert(d.isInstanceOf[ActionFailed])
    assert(e.isInstanceOf[ActionFailed])
    assert(f.isInstanceOf[ServiceStopped])
  }

  test("parallel") {
    val Vector(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { action =>
        val a1 = action("succ-1").run(IO.sleep(1.second) >> IO(1))
        val a2 = action("fail-2").updateConfig(_.withConstantDelay(1.second)).run(IO.raiseError[Int](new Exception))
        val a3 = action("cancel-3").run(IO.never[Int])
        action("supervisor")
          .updateConfig(_.withMaxRetries(1).withConstantDelay(1.second))
          .run(IO.parSequenceN(5)(List(a1, a2, a3)))
      }
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(10.second)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a1.isInstanceOf[ActionRetrying]) // a2
    assert(a2.isInstanceOf[ActionSucced] || a2.isInstanceOf[ActionRetrying]) // a1
    assert(a3.isInstanceOf[ActionRetrying] || a3.isInstanceOf[ActionSucced]) // a2
    assert(a4.isInstanceOf[ActionRetrying]) // a2
    assert(a5.isInstanceOf[ActionFailed]) // a2 failed
    assert(a6.isInstanceOf[ActionFailed]) // a3 cancelled
    //
    assert(a7.isInstanceOf[ActionRetrying]) // supervisor
    assert(a8.isInstanceOf[ActionRetrying] || a8.isInstanceOf[ActionSucced]) // a2
    assert(a9.isInstanceOf[ActionSucced] || a9.isInstanceOf[ActionRetrying]) // a1
    assert(a10.isInstanceOf[ActionRetrying]) // a2
    assert(a11.isInstanceOf[ActionRetrying]) // a2
    assert(a12.isInstanceOf[ActionFailed]) // a2 failed
    assert(a13.isInstanceOf[ActionFailed]) // a3 cancelled
    assert(a14.isInstanceOf[ActionFailed]) // supervisor
    assert(a15.isInstanceOf[ServicePanic])

  }

  test("Null point exception") {
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
}
