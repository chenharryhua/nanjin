package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard._
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

class TaskGuardTest extends AnyFunSuite {

  val guard = TaskGuard[IO]("TaskGuardTest")
    .updateServiceConfig(_.withConstantDelay(1.second))
    .updateActionConfig(_.withConstantDelay(1.second).failOn.succOn)
    .service("test")
    .updateConfig(_.withHealthCheckInterval(3.hours).withConstantDelay(1.seconds))

  val slack = SlackService(SimpleNotificationService.fake[IO])

  test("should stopped if the operation is not long run") {
    val Vector(a, b, c) = guard
      .updateConfig(_.withHealthCheckDisabled)
      .eventStream(gd =>
        gd("success action")
          .updateConfig(
            _.failOn.succOn
              .withMaxRetries(3)
              .withConstantDelay(1.second)
              .withFibonacciBackoff(1.second)
              .withFullJitter(1.second)
              .withExponentialBackoff(1.second))
          .retry(IO(1))
          .run
          .delayBy(1.second))
      .observe(_.evalMap(slack.alert).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ActionSucced])
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("should retry 2 times when operation fail") {
    var i = 0
    val Vector(a, b, c, d) = guard
      .updateConfig(_.withConstantDelay(1.hour)) // don't want to see start event
      .eventStream { gd =>
        gd("3 time")
          .updateConfig(_.withMaxRetries(3).withFullJitter(1.second))
          .retry(IO(if (i < 2) {
            i += 1; throw new Exception
          } else i))
          .run
      }
      .observe(_.evalMap(slack.alert).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionRetrying])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.asInstanceOf[ActionSucced].numRetries == 2)
    assert(d.isInstanceOf[ServiceStopped])
  }

  test("should receive 3 health check event") {
    val Vector(a, b, c, d) = guard
      .updateConfig(_.withHealthCheckInterval(1.second).withConstantDelay(1.second))
      .eventStream(_ => IO.never)
      .observe(_.evalMap(slack.alert).drain)
      .interruptAfter(5.second)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ServiceHealthCheck])
    assert(c.isInstanceOf[ServiceHealthCheck])
    assert(d.isInstanceOf[ServiceHealthCheck])
  }

  test("escalate to up level if retry failed") {
    val Vector(a, b, c, d, e) = guard
      .updateConfig(_.withConstantDelay(1.hour).withTopicMaxQueued(20)) // don't want to see start event
      .eventStream { gd =>
        gd("3 time")
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .retry(IO.raiseError(new Exception("oops")))
          .withSuccInfo((_, _: Int) => "")
          .withFailInfo((_, _) => "")
          .run
      }
      .observe(_.evalMap(slack.alert).drain)
      .interruptAfter(6.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionRetrying])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionFailed])
    assert(e.isInstanceOf[ServicePanic])
  }

  test("for your information") {
    val Vector(a) = guard
      .updateConfig(_.withConstantDelay(2.hours))
      .eventStream(gd => gd("a").fyi("hello, world") >> IO.never)
      .observe(_.evalMap(slack.alert).drain)
      .interruptAfter(2.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ForYouInformation])
  }

  test("normal service stop") {
    val Vector(a, b, c) = guard
      .updateConfig(_.withNoramlStop)
      .eventStream(gd => gd("a").retry(IO(1)).run >> gd("b").retry(IO(2)).run)
      .observe(_.evalMap(slack.alert).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionSucced])
    assert(b.isInstanceOf[ActionSucced])
    assert(c.isInstanceOf[ServiceStopped])
  }
}
