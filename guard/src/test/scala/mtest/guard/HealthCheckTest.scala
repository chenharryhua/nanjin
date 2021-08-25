package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.{
  ActionRetrying,
  ActionStart,
  ActionSucced,
  MetricsReport,
  NJEvent,
  ServiceStarted
}
import com.github.chenharryhua.nanjin.guard.observers.{console, logging}
import io.circe.parser.decode
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

class HealthCheckTest extends AnyFunSuite {
  val guard = TaskGuard[IO]("health-check")
  test("should receive 3 health check event") {
    val s :: a :: b :: c :: rest = guard
      .updateConfig(_.withZoneId(ZoneId.of("Australia/Sydney")))
      .service("normal")
      .withJmxReporter(_.inDomain("abc"))
      .updateConfig(_.withReportingSchedule("* * * ? * *"))
      .eventStream(gd => gd("cron").notice.retry(IO.never[Int]).run)
      .observe(console(_.show))
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
  }

  test("success") {
    val s :: a :: b :: c :: d :: rest = guard
      .service("success-test")
      .updateConfig(_.withReportingSchedule(1.second))
      .eventStream(gd => gd.notice.retry(IO(1)).run >> gd.notice.retry(IO.never).run)
      .observe(console(_.asJson.noSpaces))
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

  test("retry") {
    val s :: a :: b :: c :: rest = guard
      .service("failure-test")
      .updateConfig(
        _.withReportingSchedule(1.second)
          .withConstantDelay(1.hour)
          .withMetricsDurationTimeUnit(TimeUnit.MICROSECONDS)
          .withMetricsRateTimeUnit(TimeUnit.MINUTES))
      .eventStream(gd => gd("always-failure").notice.max(1).run(IO.raiseError(new Exception)) >> gd.retry(IO.never).run)
      .interruptAfter(5.second)
      .observe(logging(_.show))
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
}
