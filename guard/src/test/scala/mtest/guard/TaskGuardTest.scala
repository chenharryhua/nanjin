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

  test("should stopped if the operation normally exits") {
    val Vector(a, b, c) = guard
      .updateConfig(_.withHealthCheckDisabled.withStartUpDelay(1.second))
      .eventStream(gd =>
        gd("success action")
          .updateConfig(
            _.failOn.succOn
              .withMaxRetries(3)
              .withConstantDelay(1.second)
              .withFibonacciBackoff(1.second)
              .withFullJitter(1.second)
              .withExponentialBackoff(1.second)
          )
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
      .updateConfig(_.withStartUpDelay(1.hour)) // don't want to see start event
      .eventStream { gd =>
        gd("2 time")
          .updateConfig(_.withMaxRetries(3).withFullJitter(1.second).succOff.failOff)
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
      .updateConfig(_.withHealthCheckInterval(1.second).withStartUpDelay(1.second))
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
      .updateConfig(
        _.withStartUpDelay(1.hour).withTopicMaxQueued(20).withConstantDelay(1.hour)
      ) // don't want to see start event
      .eventStream { gd =>
        gd("3 time")
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .retry(IO.raiseError(new Exception("oops")))
          .withSuccInfo((_, _: Int) => "")
          .withFailInfo((_, _) => "")
          .run
      }
      .observe(_.evalMap(slack.alert).drain)
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

  test("for your information") {
    val Vector(a) = guard
      .updateConfig(_.withStartUpDelay(2.hours))
      .eventStream(gd => gd("fyi").fyi("hello, world") >> IO.never)
      .observe(_.evalMap(slack.alert).drain)
      .interruptAfter(5.seconds)
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

  test("retry either should give up immediately when outer action fails") {
    val Vector(a, b) = guard
      .updateConfig(_.withStartUpDelay(2.hours).withTopicMaxQueued(3).withConstantDelay(1.hour))
      .eventStream(gd =>
        gd("retry either")
          .retryEither(5)(_ => IO.raiseError(new Exception))
          .withSuccInfo((_, _: Unit) => "succ")
          .withFailInfo((_, e) => e.getMessage)
          .run)
      .observe(_.evalMap(slack.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionFailed])
    assert(b.isInstanceOf[ServicePanic])
  }

  test("retry either should retry 2 times to success") {
    var i = 0
    val Vector(a, b, c, d) = guard
      .updateConfig(_.withStartUpDelay(2.hours).withConstantDelay(1.hour))
      .eventStream(gd =>
        gd("retry either")
          .updateConfig(_.withConstantDelay(1.second).withMaxRetries(3))
          .retryEither("does not matter")(_ =>
            IO(if (i < 2) { i += 1; Left(new Exception("oops")) }
            else Right(1)))
          .run)
      .observe(_.evalMap(slack.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionRetrying])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionSucced])
    assert(d.isInstanceOf[ServiceStopped])
  }
  test("retry either should escalate if all attempts fail") {
    var i = 0
    val Vector(a, b, c, d, e) = guard
      .updateConfig(_.withStartUpDelay(2.hours).withConstantDelay(1.hour))
      .eventStream(gd =>
        gd("retry either")
          .updateConfig(_.withConstantDelay(1.second).withMaxRetries(3))
          .retryEither(IO(if (i < 200) { i += 1; Left(new Exception) }
          else Right(1)))
          .run)
      .observe(_.evalMap(slack.alert).drain)
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

  test("combine two event streams") {
    val guard = TaskGuard[IO]("two service").updateServiceConfig(_.withConstantDelay(2.hours))
    val s1    = guard.service("s1")
    val s2    = guard.service("s2")

    val ss1 = s1.eventStream(gd => gd("s1-a1").retry(IO(1)).run >> gd("s1-a2").retry(IO(2)).run)
    val ss2 = s2.eventStream(gd => gd("s2-a1").retry(IO(1)).run >> gd("s2-a2").retry(IO(2)).run)

    val vector = ss1.merge(ss2).observe(_.evalMap(slack.alert).drain).compile.toVector.unsafeRunSync()
    assert(vector.count(_.isInstanceOf[ActionSucced]) == 4)
    assert(vector.count(_.isInstanceOf[ServiceStopped]) == 2)
  }
}
