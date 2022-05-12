package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.codahale.metrics.MetricFilter
import com.github.chenharryhua.nanjin.datetime.crontabs
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.observers.{console, logging}
import eu.timepit.refined.auto.*
import io.circe.parser.decode
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

class HealthCheckTest extends AnyFunSuite {
  val guard: TaskGuard[IO] = TaskGuard[IO]("health-check")
  test("should receive 3 MetricsReport event") {
    val s :: a :: b :: c :: rest = guard
      .updateConfig(_.withZoneId(ZoneId.of("Australia/Sydney")))
      .service("normal")
      .withJmxReporter(_.inDomain("abc"))
      .withMetricFilter(MetricFilter.startsWith("01"))
      .updateConfig(_.withMetricReport(2.seconds))
      .eventStream(gd => gd.span("cron").notice.retry(IO.never[Int]).run)
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .interruptAfter(9.second)
      .evalTap(logging.simple[IO])
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[MetricReport])
    assert(c.isInstanceOf[MetricReport])
    assert(c.isInstanceOf[MetricReport])
  }

  test("success-test") {
    val s :: a :: b :: c :: d :: rest = guard
      .service("success-test")
      .updateConfig(_.withMetricReport(1.second))
      .eventStream(gd => gd.notice.retry(IO(1)).run >> gd.notice.retry(IO.never).run)
      .evalTap(console.simple[IO])
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .evalTap(logging.simple[IO])
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionSucc])
    assert(c.isInstanceOf[ActionStart])
    assert(d.isInstanceOf[MetricReport])
  }

  test("never success") {
    val s :: a :: b :: c :: rest = guard
      .service("metrics-report")
      .updateConfig(
        _.withMetricReport(1.second)
          .withConstantDelay(1.hour)
          .withMetricDurationTimeUnit(TimeUnit.MICROSECONDS)
          .withMetricRateTimeUnit(TimeUnit.MINUTES))
      .eventStream(gd =>
        gd.span("not")
          .span("fail")
          .span("yet")
          .updateConfig(_.withConstantDelay(300.second).withCapDelay(2.seconds))
          .notice
          .max(10)
          .run(IO.raiseError(new Exception)))
      .interruptAfter(5.second)
      .evalTap(logging.simple[IO])
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .compile
      .toList
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[MetricReport])
  }

  test("metrics reset") {
    val list = guard
      .service("metrics-reset-test")
      .updateConfig(_.withMetricReport(2.seconds).withMetricReset(crontabs.trisecondly))
      .eventStream(_.run(IO.never))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .evalTap(logging.simple[IO])
      .interruptAfter(7.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(list.nonEmpty)
  }
}
