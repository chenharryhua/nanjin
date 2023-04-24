package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.{monthlyCron, weeklyCron, MetricParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.service.{NameConstraint, ServiceGuard}
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.{ZoneId, ZonedDateTime}
import java.util.concurrent.TimeUnit

class ConfigTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("config")
      .service("config")
      .updateConfig(_.withMetricDailyReset.withMetricReport(cron_1hour))

  test("1.counting") {
    val as = service.eventStream { agent =>
      agent.action("cfg", _.notice.withCounting).retry(IO(1)).run
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(as.actionParams.isCounting)
  }
  test("2.without counting") {
    val as = service.eventStream { agent =>
      agent.action("cfg", _.notice.withoutCounting).retry(IO(1)).run
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(!as.actionParams.isCounting)
  }

  test("3.timing") {
    val as = service.eventStream { agent =>
      agent.action("cfg", _.notice.withTiming).retry(IO(1)).run
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(as.actionParams.isTiming)
  }

  test("4.without timing") {
    val as = service.eventStream { agent =>
      agent.action("cfg", _.notice.withoutTiming).retry(IO(1)).run
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(!as.actionParams.isTiming)
  }

  test("8.silent") {
    val as = service.eventStream { agent =>
      agent.action("cfg", _.silent).retry(IO(1)).run
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync()
    assert(as.isEmpty)
  }

  test("9.report") {
    val as = service
      .updateConfig(_.withoutMetricReport)
      .eventStream { agent =>
        agent.action("cfg", _.silent).retry(IO(1)).run
      }
      .filter(_.isInstanceOf[ServiceStart])
      .compile
      .last
      .unsafeRunSync()
    assert(as.get.serviceParams.metricParams.reportSchedule.isEmpty)
  }

  test("10.reset") {
    val as = service
      .updateConfig(_.withoutMetricReset)
      .eventStream { agent =>
        agent.action("cfg", _.silent).retry(IO(1)).run
      }
      .filter(_.isInstanceOf[ServiceStart])
      .compile
      .last
      .unsafeRunSync()
    assert(as.get.serviceParams.metricParams.resetSchedule.isEmpty)
  }

  test("11.MonthlyReset - 00:00:01 of 1st day of the month") {
    val zoneId = ZoneId.of("Australia/Sydney")
    val metricParams =
      MetricParams(None, Some(monthlyCron), "", TimeUnit.MINUTES, TimeUnit.MINUTES)
    val now      = ZonedDateTime.of(2022, 10, 26, 0, 0, 0, 0, zoneId)
    val ns       = metricParams.nextReset(now).get
    val expected = ZonedDateTime.of(2022, 11, 1, 0, 0, 1, 0, zoneId)
    assert(ns === expected)
  }

  test("12.WeeklyReset - 00:00:01 on Monday") {
    val zoneId = ZoneId.of("Australia/Sydney")
    val metricParams =
      MetricParams(None, Some(weeklyCron), "", TimeUnit.MINUTES, TimeUnit.MINUTES)
    val now      = ZonedDateTime.of(2022, 10, 26, 0, 0, 0, 0, zoneId)
    val ns       = metricParams.nextReset(now).get
    val expected = ZonedDateTime.of(2022, 10, 31, 0, 0, 1, 0, zoneId)
    assert(ns === expected)
  }

  test("composable service config") {
    val homepage = service
      .updateConfig(_.withHomePage("abc"))
      .updateConfig(_.withMetricDailyReset)
      .eventStream(_ => IO(1))
      .filter(_.isInstanceOf[ServiceStart])
      .compile
      .last
      .unsafeRunSync()
      .get
      .serviceParams
      .homePage
      .get
    assert(homepage == "abc")
  }

  test("composable action config") {
    val as = service
      .eventStream(_.action("abc", _.notice.withCounting).updateConfig(_.withTiming).delay(1).run)
      .filter(_.isInstanceOf[ActionStart])
      .compile
      .last
      .unsafeRunSync()
      .get
      .asInstanceOf[ActionStart]

    assert(as.actionParams.isCounting)
    assert(as.actionParams.isTiming)
  }
  test("should not contain {}") {
    assertThrows[IllegalArgumentException](NameConstraint.unsafeFrom("{a b c}"))
    NameConstraint.unsafeFrom(" a B 3 , . _ - / \\ ! @ # $ % & + * = < > ? ^ : ").value
  }
}
