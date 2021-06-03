package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard._
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionFailed,
  ActionRetrying,
  ActionSucced,
  MetricsService,
  ServicePanic,
  ServiceStoppedAbnormally,
  SlackService
}
import org.scalatest.funsuite.AnyFunSuite
import cats.syntax.all._
import scala.concurrent.duration._

class RetryTest extends AnyFunSuite {

  val guard = TaskGuard[IO]("retry-guard")
    .updateServiceConfig(_.withConstantDelay(1.second))
    .updateActionConfig(_.withConstantDelay(1.second).withFailAlertOn.withSuccAlertOn)
    .service("retry-test")
    .updateServiceConfig(_.withHealthCheckInterval(3.hours).withConstantDelay(1.seconds))

  val logging = SlackService(SimpleNotificationService.fake[IO]) |+| MetricsService[IO](new MetricRegistry())

  test("should retry 2 times when operation fail") {
    var i = 0
    val Vector(a, b, c, d) = guard
      .updateServiceConfig(_.withStartUpDelay(1.hour)) // don't want to see start event
      .eventStream { gd =>
        gd("2-time-succ")
          .updateActionConfig(_.withMaxRetries(3).withFullJitter(1.second).withSuccAlertOff.withFailAlertOff)
          .retry(IO(if (i < 2) {
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
    assert(d.isInstanceOf[ServiceStoppedAbnormally])
  }

  test("should escalate to up level if retry failed") {
    val Vector(a, b, c, d, e) = guard
      .updateServiceConfig(
        _.withStartUpDelay(1.hour).withTopicMaxQueued(20).withConstantDelay(1.hour)
      ) // don't want to see start event
      .eventStream { gd =>
        gd("escalate-after-3-time")
          .updateActionConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
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
}
