package com.github.chenharryhua.nanjin.guard.service

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.HostName.local_host
import com.github.chenharryhua.nanjin.common.utils.zzffEpoch
import com.github.chenharryhua.nanjin.guard.config.{
  MetricParams,
  Policy,
  ServiceName,
  ServiceParams,
  TaskParams
}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot}
import com.github.chenharryhua.nanjin.guard.observers.sampling
import cron4s.Cron
import cron4s.expr.CronExpr
import mtest.guard.{beijingTime, cron_1minute, cron_1second, cron_2second}
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import java.time.ZonedDateTime
import java.util.UUID
import scala.concurrent.duration.*

class MetricSamplingTest extends AnyFunSuite {

  val serviceParams: ServiceParams = ServiceParams(
    ServiceName("sampling"),
    TaskParams("name", beijingTime, local_host),
    UUID.randomUUID(),
    ZonedDateTime.of(zzffEpoch, beijingTime).toInstant,
    Policy(RetryPolicies.alwaysGiveUp[IO]),
    None
  )

  def metricReport(cron: CronExpr, now: ZonedDateTime): MetricReport = MetricReport(
    MetricIndex.Periodic(1023),
    ServiceParams.metricParams.composeLens(MetricParams.reportSchedule).set(Some(cron))(serviceParams),
    now,
    MetricSnapshot(Nil, Nil, Nil, Nil, Nil)
  )

  test("1.cron secondly") {
    val ts: ZonedDateTime = ZonedDateTime.of(2021, 1, 1, 0, 0, 0, 0, beijingTime)
    val ts1               = ts.plusSeconds(1).plusNanos(10)
    val ts2               = ts.plusSeconds(2)
    val ts3               = ts.plusSeconds(3)
    val ts4               = ts.plusSeconds(4)
    val ts5               = ts.plusSeconds(5).plusNanos(10)
    val ts6               = ts.plusSeconds(6)
    val ts7               = ts.plusSeconds(7)
    val ts8               = ts.plusSeconds(8)
    val ts9               = ts.plusSeconds(9).plusNanos(10)
    val ts10              = ts.plusSeconds(10)

    val interval = 3.second
    assert(!sampling(interval)(metricReport(cron_1second, ts1)))
    assert(!sampling(interval)(metricReport(cron_1second, ts2)))
    assert(sampling(interval)(metricReport(cron_1second, ts3)))
    assert(!sampling(interval)(metricReport(cron_1second, ts4)))
    assert(!sampling(interval)(metricReport(cron_1second, ts5)))
    assert(sampling(interval)(metricReport(cron_1second, ts6)))
    assert(!sampling(interval)(metricReport(cron_1second, ts7)))
    assert(!sampling(interval)(metricReport(cron_1second, ts8)))
    assert(sampling(interval)(metricReport(cron_1second, ts9)))
    assert(!sampling(interval)(metricReport(cron_1second, ts10)))
  }

  test("2.cron bi-secondly") {
    val ts: ZonedDateTime = ZonedDateTime.of(2021, 1, 1, 0, 0, 0, 0, beijingTime)
    val ts1               = ts.plusSeconds(2).plusNanos(10)
    val ts2               = ts.plusSeconds(4)
    val ts3               = ts.plusSeconds(6)
    val ts4               = ts.plusSeconds(8)
    val ts5               = ts.plusSeconds(10).plusNanos(10)
    val ts6               = ts.plusSeconds(12)
    val ts7               = ts.plusSeconds(14)
    val ts8               = ts.plusSeconds(16)
    val ts9               = ts.plusSeconds(18).plusNanos(10)
    val ts10              = ts.plusSeconds(20)

    val interval = 3.second
    assert(!sampling(interval)(metricReport(cron_2second, ts1)))
    assert(sampling(interval)(metricReport(cron_2second, ts2)))
    assert(sampling(interval)(metricReport(cron_2second, ts3)))
    assert(!sampling(interval)(metricReport(cron_2second, ts4)))
    assert(sampling(interval)(metricReport(cron_2second, ts5)))
    assert(sampling(interval)(metricReport(cron_2second, ts6)))
    assert(!sampling(interval)(metricReport(cron_2second, ts7)))
    assert(sampling(interval)(metricReport(cron_2second, ts8)))
    assert(sampling(interval)(metricReport(cron_2second, ts9)))
    assert(!sampling(interval)(metricReport(cron_2second, ts10)))
  }

  test("3.cron sampling - cron") {
    val ts: ZonedDateTime = ZonedDateTime.of(2021, 1, 1, 16, 0, 0, 0, beijingTime)
    val ts1               = ts.minusMinutes(2).plusNanos(10)
    val ts2               = ts.minusMinutes(2)
    val ts3               = ts.minusMinutes(1).plusNanos(10)
    val ts4               = ts.minusMinutes(1)
    val ts5               = ts
    val ts6               = ts.plusMinutes(1)
    val ts7               = ts.plusMinutes(1).plusNanos(10)
    val ts8               = ts.plusMinutes(2)
    val ts9               = ts.plusMinutes(2).plusNanos(10)

    val hourly: CronExpr = Cron.unsafeParse("0 0 0-23 ? * *")
    assert(!sampling(hourly)(metricReport(cron_1minute, ts1)))
    assert(!sampling(hourly)(metricReport(cron_1minute, ts2)))
    assert(sampling(hourly)(metricReport(cron_1minute, ts3)))
    assert(sampling(hourly)(metricReport(cron_1minute, ts4)))
    assert(!sampling(hourly)(metricReport(cron_1minute, ts5)))
    assert(!sampling(hourly)(metricReport(cron_1minute, ts6)))
    assert(!sampling(hourly)(metricReport(cron_1minute, ts7)))
    assert(!sampling(hourly)(metricReport(cron_1minute, ts8)))
    assert(!sampling(hourly)(metricReport(cron_1minute, ts9)))
  }
}
