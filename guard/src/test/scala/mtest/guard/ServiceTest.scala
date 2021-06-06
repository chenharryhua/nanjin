package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard._
import org.scalatest.funsuite.AnyFunSuite
import cats.syntax.all._
import com.amazonaws.regions.Regions
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionFailed,
  ActionRetrying,
  ActionSucced,
  ForYouInformation,
  LogService,
  MetricsService,
  NJEvent,
  ServiceHealthCheck,
  ServicePanic,
  ServiceStarted,
  ServiceStoppedAbnormally,
  SlackService
}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import io.circe.syntax._
import io.circe.parser.decode

class ServiceTest extends AnyFunSuite {
  val slack  = SlackService[IO](SnsArn("arn:aws:sns:ap-southeast-2:123456789012:abc-123xyz"))
  val slack2 = SlackService[IO](SnsArn("arn:aws:sns:ap-southeast-2:123456789012:abc-123xyz"), Regions.AP_SOUTHEAST_2)

  val guard = TaskGuard[IO]("service-level-guard")
    .updateServiceConfig(_.withConstantDelay(1.second))
    .updateActionConfig(_.withConstantDelay(1.second).withFailAlertOn.withSuccAlertOn)
    .service("service")
    .updateServiceConfig(_.withHealthCheckInterval(3.hours).withConstantDelay(1.seconds))

  val metrics = new MetricRegistry
  val logging = SlackService(SimpleNotificationService.fake[IO]) |+| MetricsService[IO](metrics) |+| LogService[IO]

  test("should stopped if the operation normally exits") {
    val Vector(a, b, c) = guard
      .updateServiceConfig(_.withHealthCheckDisabled.withStartUpDelay(1.second))
      .eventStream(gd =>
        gd("normal-exit-action")
          .updateActionConfig(_.withFailAlertOn.withSuccAlertOff.withMaxRetries(3).withExponentialBackoff(1.second))
          .retry(IO(1))
          .run
          .delayBy(1.second))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .observe(_.evalMap(m => logging.alert(m)).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ActionSucced])
    assert(c.isInstanceOf[ServiceStoppedAbnormally])
  }

  test("should receive 3 health check event") {
    val Vector(a, b, c, d) = guard
      .updateServiceConfig(_.withHealthCheckInterval(1.second).withStartUpDelay(1.second))
      .eventStream(_ => IO.never)
      .observe(_.evalMap(m => logging.alert(m)).drain)
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
      .updateServiceConfig(_.withStartUpDelay(1.hour).withConstantDelay(1.hour))
      .eventStream { gd =>
        gd("escalate-after-3-time")
          .updateActionConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .retry(IO.raiseError(new Exception("oops")))
          .withSuccNotes((_, _: Int) => null)
          .withFailNotes((_, _) => null)
          .run
      }
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .observe(_.evalMap(m => logging.alert(m) >> IO.println(m.show)).drain)
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
      .updateServiceConfig(_.withStartUpDelay(2.hours))
      .eventStream(_.fyi("hello, world") >> IO.never)
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .observe(_.evalMap(m => logging.alert(m)).drain)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ForYouInformation])
  }

  test("normal service stop after two operations") {
    val Vector(a, b, c) = guard
      .eventStream(gd => gd("a").retry(IO(1)).run >> gd("b").retry(IO(2)).run)
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .observe(_.evalMap(m => logging.alert(m)).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionSucced])
    assert(b.isInstanceOf[ActionSucced])
    assert(c.isInstanceOf[ServiceStoppedAbnormally])
  }

  test("combine two event streams") {
    val guard = TaskGuard[IO]("two service").updateServiceConfig(_.withConstantDelay(2.hours))
    val s1    = guard.service("s1")
    val s2    = guard.service("s2")

    val ss1 = s1.eventStream(gd => gd("s1-a1").retry(IO(1)).run >> gd("s1-a2").retry(IO(2)).run)
    val ss2 = s2.eventStream(gd => gd("s2-a1").retry(IO(1)).run >> gd("s2-a2").retry(IO(2)).run)

    val vector =
      ss1.merge(ss2).observe(_.evalMap(m => logging.alert(m)).drain).compile.toVector.unsafeRunSync()
    assert(vector.count(_.isInstanceOf[ActionSucced]) == 4)
    assert(vector.count(_.isInstanceOf[ServiceStoppedAbnormally]) == 2)
  }

  test("metrics success count") {
    val guard = TaskGuard[IO]("metrics-test")
    val s     = guard.service("metrics-service")
    s.eventStream { gd =>
      (gd("metrics-action-succ").retry(IO(1) >> IO.sleep(10.milliseconds)).run).replicateA(20)
    }.observe(_.evalMap(m => logging.alert(m)).drain).compile.drain.unsafeRunSync()

    metrics.getMetrics.asScala.filter(_._1.contains("metrics-action-succ")).map { case (n, m) =>
      m match {
        case t: Timer   => assert(t.getCount() == 20)
        case c: Counter => assert(c.getCount() == 20)
      }
    }
  }

  test("metrics failure count") {
    val guard = TaskGuard[IO]("metrics-test")
    val s     = guard.service("metrics-service")
    s.eventStream { gd =>
      (gd("metrics-action-fail")
        .updateActionConfig(_.withConstantDelay(10.milliseconds))
        .retry(IO.raiseError(new Exception))
        .run)
        .replicateA(20)
    }.observe(_.evalMap(m => logging.alert(m)).drain).interruptAfter(5.seconds).compile.drain.unsafeRunSync()

    metrics.getMetrics.asScala.filter(_._1.contains("metrics-action-fail.counter.retry")).map { case (n, m) =>
      m match {
        case c: Counter => assert(c.getCount() == 3)
      }
    }
  }
}
