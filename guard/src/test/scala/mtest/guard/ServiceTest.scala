package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.amazonaws.regions.Regions
import com.codahale.metrics.{Counter, Timer}
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.alert.{
  toOrdinalWords,
  ActionFailed,
  ActionRetrying,
  ActionStart,
  ActionSucced,
  NJEvent,
  ServiceHealthCheck,
  ServicePanic,
  ServiceStarted,
  ServiceStopped,
  SlackService
}
import com.github.chenharryhua.nanjin.guard.config.{ConstantDelay, FibonacciBackoff}
import eu.timepit.refined.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters.*
import scala.concurrent.duration.*
class ServiceTest extends AnyFunSuite {
  val slackNoUse1 = SlackService[IO](SnsArn("arn:aws:sns:ap-southeast-2:123456789012:abc-123xyz"))

  val slackNoUse2 = SlackService[IO](
    SnsArn("arn:aws:sns:ap-southeast-2:123456789012:abc-123xyz"),
    Regions.AP_SOUTHEAST_2,
    DurationFormatter.defaultFormatter)

  val guard = TaskGuard[IO]("service-level-guard")
    .updateConfig(
      _.withSlackWarnColor("danger")
        .withSlackFailColor("danger")
        .withSlackSuccColor("good")
        .withSlackInfoColor("good")
        .withHostName(HostName.local_host)
        .withDailySummaryResetDisabled
        .withDailySummaryReset(1))
    .service("service")
    .updateConfig(_.withHealthCheckInterval(3.hours)
      .withConstantDelay(1.seconds)
      .withSlackMaximumCauseSize(100)
      .withStartupNotes("ok"))

  test("should stopped if the operation normally exits") {
    val Vector(a, b, c, d) = guard
      .updateConfig(_.withStartupDelay(0.second).withJitterBackoff(3.second))
      .withAlert(metrics)
      .withAlert(log)
      .withAlert(console)
      .withAlert(slack)
      .eventStream(gd => gd("normal-exit-action").max(10).magpie(IO(1))(_ => null).delayBy(1.second))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionSucced])
    assert(d.isInstanceOf[ServiceStopped])
  }

  test("escalate to up level if retry failed") {
    val Vector(a, b, c, d, e, f) = guard
      .updateConfig(_.withJitterBackoff(30.minutes, 1.hour))
      .eventStream { gd =>
        gd("escalate-after-3-time")
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second).withSlackRetryOn)
          .croak(IO.raiseError(new Exception("oops")))(_ => null)
      }
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
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

  test("should receive 3 health check event") {
    val a :: b :: c :: d :: e :: rest = guard
      .updateConfig(_.withHealthCheckInterval(1.second).withStartupDelay(0.1.second))
      .eventStream(_.run(IO.never))
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ServiceStarted])
    assert(c.isInstanceOf[ServiceHealthCheck])
    assert(d.isInstanceOf[ServiceHealthCheck])
    assert(e.isInstanceOf[ServiceHealthCheck])
  }

  test("normal service stop after two operations") {
    val Vector(a, b, c, d, e) = guard
      .eventStream(gd => gd("a").retry(IO(1)).run >> gd("b").retry(IO(2)).run)
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionSucced])
    assert(c.isInstanceOf[ActionStart])
    assert(d.isInstanceOf[ActionSucced])
    assert(e.isInstanceOf[ServiceStopped])
  }

  test("combine two event streams") {
    val guard = TaskGuard[IO]("two service")
    val s1    = guard.service("s1")
    val s2    = guard.service("s2")

    val ss1 = s1.eventStream(gd => gd("s1-a1").retry(IO(1)).run >> gd("s1-a2").retry(IO(2)).run)
    val ss2 = s2.eventStream(gd => gd("s2-a1").retry(IO(1)).run >> gd("s2-a2").retry(IO(2)).run)

    val vector = ss1.merge(ss2).compile.toVector.unsafeRunSync()
    assert(vector.count(_.isInstanceOf[ActionSucced]) == 4)
    assert(vector.count(_.isInstanceOf[ServiceStopped]) == 2)
  }

  test("metrics success count") {
    val guard = TaskGuard[IO]("metrics-test")
    val s     = guard.service("metrics-service").withAlert(metrics)
    s.eventStream { gd =>
      (gd("metrics-action-succ").retry(IO(1) >> IO.sleep(10.milliseconds)).run).replicateA(20)
    }.compile.drain.unsafeRunSync()

    metricRegistry.getMetrics.asScala.filter(_._1.contains("metrics-action-succ")).map { case (n, m) =>
      m match {
        case t: Timer   => assert(t.getCount == 20)
        case c: Counter => assert(c.getCount == 20)
      }
    }
  }

  test("metrics failure count") {
    val guard = TaskGuard[IO]("metrics-test")
    val s     = guard.service("metrics-service").withAlert(metrics)
    s.eventStream { gd =>
      (gd("metrics-action-fail")
        .updateConfig(_.withConstantDelay(10.milliseconds))
        .retry(IO.raiseError(new Exception))
        .run)
        .replicateA(20)
    }.interruptAfter(5.seconds).compile.drain.unsafeRunSync()

    metricRegistry.getMetrics.asScala.filter(_._1.contains("metrics-action-fail.counter.retry")).map { case (n, m) =>
      m match {
        case c: Counter => assert(c.getCount == 3)
      }
    }
  }

  test("toWords") {
    assert(toOrdinalWords(1) == "1st")
    assert(toOrdinalWords(2) == "2nd")
    assert(toOrdinalWords(10) == "10th")
  }

  test("zoneId ") {
    guard
      .eventStream(ag => IO(assert(ag.zoneId == ag.params.serviceParams.taskParams.zoneId)))
      .compile
      .drain
      .unsafeRunSync()
  }
  test("all alert on") {
    guard.eventStream { ag =>
      val g = ag("").updateConfig(_.withSlackNone.withSlackAll.withMaxRetries(5))
      g.run(IO {
        assert(g.params.alertMask.alertStart)
        assert(g.params.alertMask.alertSucc)
        assert(g.params.alertMask.alertFail)
        assert(g.params.alertMask.alertFirstRetry)
        assert(g.params.alertMask.alertRetry)
        assert(g.params.shouldTerminate)
        assert(g.params.retry.maxRetries == 5)
        assert(g.params.retry.njRetryPolicy.isInstanceOf[ConstantDelay])
      })
    }.compile.drain.unsafeRunSync()
  }
  test("all alert off") {
    guard.eventStream { ag =>
      val g = ag("").updateConfig(_.withSlackAll.withSlackNone.withNonTermination.withFibonacciBackoff(1.second))
      g.run(IO {
        assert(!g.params.alertMask.alertStart)
        assert(!g.params.alertMask.alertSucc)
        assert(!g.params.alertMask.alertFail)
        assert(!g.params.alertMask.alertFirstRetry)
        assert(!g.params.alertMask.alertRetry)
        assert(!g.params.shouldTerminate)
        assert(g.params.retry.maxRetries == 0)
        assert(g.params.retry.njRetryPolicy.isInstanceOf[FibonacciBackoff])
      })
    }.interruptAfter(3.seconds).compile.drain.unsafeRunSync()
  }
}
