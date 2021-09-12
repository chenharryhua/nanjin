package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.codahale.metrics.MetricFilter
import com.github.chenharryhua.nanjin.datetime.crontabs
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.observers.{console, logging}
import io.circe.parser.decode
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

class HealthCheckTest extends AnyFunSuite {
  val guard: TaskGuard[IO] = TaskGuard[IO]("health-check")
  test("should receive 3 health check event") {
    val s :: a :: b :: c :: rest = guard
      .updateConfig(_.withZoneId(ZoneId.of("Australia/Sydney")))
      .service("normal")
      .withJmxReporter(_.inDomain("abc"))
      .withMetricFilter(MetricFilter.startsWith("01"))
      .updateConfig(_.withMetricSchedule("* * * ? * *"))
      .eventStream(gd => gd.span("cron").notice.retry(IO.never[Int]).run)
      .evalTap(console[IO](_.show))
      .evalTap(console[IO](_.asJson.noSpaces))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[MetricsReport])
    assert(c.isInstanceOf[MetricsReport])
    assert(c.asInstanceOf[MetricsReport].snapshot.counters.keys.toList.contains("01.health.check"))
    assert(!c.asInstanceOf[MetricsReport].snapshot.counters.keys.toList.contains("02.service"))
  }

  test("success-test") {
    val s :: a :: b :: c :: d :: rest = guard
      .service("success-test")
      .updateConfig(_.withMetricSchedule(1.second))
      .eventStream(gd => gd.notice.retry(IO(1)).run >> gd.notice.retry(IO.never).run)
      .evalTap(console[IO](_.asJson.noSpaces))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionSucced])
    assert(c.isInstanceOf[ActionStart])
    assert(d.isInstanceOf[MetricsReport])
  }

  test("always-failure") {
    val s :: a :: b :: c :: rest = guard
      .service("always-failure")
      .updateConfig(
        _.withMetricSchedule(1.second)
          .withConstantDelay(1.hour)
          .withMetricDurationTimeUnit(TimeUnit.MICROSECONDS)
          .withMetricRateTimeUnit(TimeUnit.MINUTES))
      .eventStream(gd =>
        gd.span("always-failure")
          .updateConfig(_.withConstantDelay(3.second))
          .notice
          .max(10)
          .run(IO.raiseError(new Exception)))
      .interruptAfter(5.second)
      .evalTap(logging[IO](_.show))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .compile
      .toList
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[MetricsReport])
  }

  test("metrics reset") {
    val list = guard
      .service("metrics-reset-test")
      .updateConfig(_.withMetricSchedule(2.seconds).withMetricReset(crontabs.trisecondly))
      .eventStream(_.run(IO.never))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .debug()
      .interruptAfter(7.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(list.nonEmpty)
  }
}
