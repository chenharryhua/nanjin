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
  test("should receive 3 MetricsReport event") {
    val s :: a :: b :: c :: rest = guard
      .updateConfig(_.withZoneId(ZoneId.of("Australia/Sydney")))
      .service("normal")
      .withJmxReporter(_.inDomain("abc"))
      .withMetricFilter(MetricFilter.startsWith("01"))
      .updateConfig(_.withMetricReport("* * * ? * *"))
      .eventStream(gd => gd.span("cron").notice.retry(IO.never[Int]).run)
      .evalTap(console.json[IO](_.noSpaces))
      .evalTap(console.text[IO])
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .interruptAfter(7.second)
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[MetricsReport])
    assert(c.isInstanceOf[MetricsReport])
    assert(c.isInstanceOf[MetricsReport])
  }

  test("success-test") {
    val s :: a :: b :: c :: d :: rest = guard
      .service("success-test")
      .updateConfig(_.withMetricReport(1.second))
      .eventStream(gd => gd.notice.retry(IO(1)).run >> gd.notice.retry(IO.never).run)
      .evalTap(console.json[IO](_.spaces2))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionSucc])
    assert(c.isInstanceOf[ActionStart])
    assert(d.isInstanceOf[MetricsReport])
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
      .evalTap(logging.text[IO])
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .compile
      .toList
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[MetricsReport])
  }

  test("metrics reset") {
    val list = guard
      .service("metrics-reset-test")
      .updateConfig(_.withMetricReport(2.seconds).withMetricReset(crontabs.trisecondly))
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
