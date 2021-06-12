package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import com.amazonaws.regions.Regions
import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard._
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
  ServiceStopped,
  SlackService
}
import io.circe.parser.decode
import io.circe.syntax._
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class ServiceTest extends AnyFunSuite {
  val slack = SlackService[IO](SnsArn("arn:aws:sns:ap-southeast-2:123456789012:abc-123xyz"))

  val slack2 = SlackService[IO](
    SnsArn("arn:aws:sns:ap-southeast-2:123456789012:abc-123xyz"),
    Regions.AP_SOUTHEAST_2,
    DurationFormatter.default)

  val guard = TaskGuard[IO]("service-level-guard")
    .service("service")
    .updateConfig(_.withHealthCheckInterval(3.hours).withConstantDelay(1.seconds))

  val metrics = new MetricRegistry
  val logging = SlackService(SimpleNotificationService.fake[IO]) |+| MetricsService[IO](metrics) |+| LogService[IO]

  test("should stopped if the operation normally exits") {
    val Vector(a, b, c) = guard
      .updateConfig(_.withStartUpDelay(0.second).withJitter(3.second))
      .eventStream(gd => gd("normal-exit-action").max(10).magpie(IO(1))(_ => null).delayBy(1.second))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .observe(_.evalMap(m => logging.alert(m)).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ActionSucced])
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("escalate to up level if retry failed") {
    val Vector(a, b, c, d, e) = guard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { gd =>
        gd("escalate-after-3-time")
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .croak(IO.raiseError(new Exception("oops")))(_ => null)
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

  test("should receive 3 health check event") {
    val a :: b :: c :: d :: rest = guard
      .updateConfig(_.withHealthCheckInterval(1.second).withStartUpDelay(0.1.second))
      .eventStream(_.run(IO.never))
      .observe(_.evalMap(m => logging.alert(m)).drain)
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ServiceHealthCheck])
    assert(c.isInstanceOf[ServiceHealthCheck])
    assert(d.isInstanceOf[ServiceHealthCheck])
  }

  test("for your information") {
    val Vector(a) = guard
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
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("combine two event streams") {
    val guard = TaskGuard[IO]("two service")
    val s1    = guard.service("s1")
    val s2    = guard.service("s2")

    val ss1 = s1.eventStream(gd => gd("s1-a1").retry(IO(1)).run >> gd("s1-a2").retry(IO(2)).run)
    val ss2 = s2.eventStream(gd => gd("s2-a1").retry(IO(1)).run >> gd("s2-a2").retry(IO(2)).run)

    val vector =
      ss1.merge(ss2).observe(_.evalMap(m => logging.alert(m)).drain).compile.toVector.unsafeRunSync()
    assert(vector.count(_.isInstanceOf[ActionSucced]) == 4)
    assert(vector.count(_.isInstanceOf[ServiceStopped]) == 2)
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
        .updateConfig(_.withConstantDelay(10.milliseconds))
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
