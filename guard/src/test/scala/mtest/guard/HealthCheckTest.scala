package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.codahale.metrics.jvm.MemoryUsageGaugeSet
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.{
  ActionRetrying,
  ActionStart,
  ActionSucced,
  MetricRegistryWrapper,
  MetricsReport,
  ServiceStarted
}
import com.github.chenharryhua.nanjin.guard.observers.{jsonConsole, metricConsole, showConsole, showLog}
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalTime, ZoneId}
import scala.concurrent.duration.*

class HealthCheckTest extends AnyFunSuite {
  val guard = TaskGuard[IO]("health-check")
  test("should receive 3 health check event") {
    val s :: a :: b :: c :: rest = guard
      .updateConfig(_.withZoneId(ZoneId.of("Australia/Sydney")))
      .service("normal")
      .withJmxReporter(_.inDomain("abc"))
      .updateConfig(_.withReportingSchedule("* * * ? * *"))
      .eventStream(gd => gd("cron").retry(IO.never[Int]).run(()))
      .observe(showConsole)
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
      .eventStream(gd => gd.retry(IO(1)).run(()) >> gd.retry(IO.never).run(()))
      .observe(jsonConsole)
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
      .updateConfig(_.withReportingSchedule(1.second).withConstantDelay(1.hour))
      .eventStream(gd =>
        gd("always-failure").max(1).retry(IO.raiseError(new Exception)).run(()) >> gd.retry(IO.never).run(()))
      .interruptAfter(5.second)
      .observe(showLog)
      .compile
      .toList
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[MetricsReport])
  }
}
