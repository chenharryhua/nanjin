package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.utils.zzffEpoch
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.{MetricParams, ScheduleType, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{MetricReportType, MetricSnapshot}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.observers.sampling
import cron4s.Cron
import eu.timepit.refined.auto.*
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZonedDateTime
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.ScalaDurationOps

class MetricsTimingTest extends AnyFunSuite {
  val launchTime: ZonedDateTime = ZonedDateTime.of(zzffEpoch, beijingTime)

  val serviceParams: ServiceParams =
    ServiceParams.launchTime.set(launchTime)(
      TaskGuard[IO]("test").service("sampling").dummyAgent.use(a => IO(a.serviceParams)).unsafeRunSync())

  def metricReport(st: Option[ScheduleType], now: ZonedDateTime): MetricReport = MetricReport(
    MetricReportType.Scheduled(1023),
    ServiceParams.metric.composeLens(MetricParams.reportSchedule).set(st)(serviceParams),
    now,
    MetricSnapshot(Map.empty, Json.Null, "")
  )

  test("fixed rate secondly") {
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

    val scheduleType = Some(ScheduleType.Fixed(1.second.toJava))
    val interval     = 3.second
    assert(!sampling(interval)(metricReport(scheduleType, ts1)))
    assert(!sampling(interval)(metricReport(scheduleType, ts2)))
    assert(sampling(interval)(metricReport(scheduleType, ts3)))
    assert(!sampling(interval)(metricReport(scheduleType, ts4)))
    assert(!sampling(interval)(metricReport(scheduleType, ts5)))
    assert(sampling(interval)(metricReport(scheduleType, ts6)))
    assert(!sampling(interval)(metricReport(scheduleType, ts7)))
    assert(!sampling(interval)(metricReport(scheduleType, ts8)))
    assert(sampling(interval)(metricReport(scheduleType, ts9)))
    assert(!sampling(interval)(metricReport(scheduleType, ts10)))
  }

  test("fixed rate bi-secondly") {
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

    val scheduleType = Some(ScheduleType.Fixed(2.second.toJava))
    val interval     = 3.second
    assert(!sampling(interval)(metricReport(scheduleType, ts1)))
    assert(sampling(interval)(metricReport(scheduleType, ts2)))
    assert(sampling(interval)(metricReport(scheduleType, ts3)))
    assert(!sampling(interval)(metricReport(scheduleType, ts4)))
    assert(sampling(interval)(metricReport(scheduleType, ts5)))
    assert(sampling(interval)(metricReport(scheduleType, ts6)))
    assert(!sampling(interval)(metricReport(scheduleType, ts7)))
    assert(sampling(interval)(metricReport(scheduleType, ts8)))
    assert(sampling(interval)(metricReport(scheduleType, ts9)))
    assert(!sampling(interval)(metricReport(scheduleType, ts10)))
  }

  test("cron secondly") {
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

    val scheduleType = Some(ScheduleType.Cron(Cron.unsafeParse("* * * ? * *")))
    val interval     = 3.second
    assert(!sampling(interval)(metricReport(scheduleType, ts1)))
    assert(!sampling(interval)(metricReport(scheduleType, ts2)))
    assert(sampling(interval)(metricReport(scheduleType, ts3)))
    assert(!sampling(interval)(metricReport(scheduleType, ts4)))
    assert(!sampling(interval)(metricReport(scheduleType, ts5)))
    assert(sampling(interval)(metricReport(scheduleType, ts6)))
    assert(!sampling(interval)(metricReport(scheduleType, ts7)))
    assert(!sampling(interval)(metricReport(scheduleType, ts8)))
    assert(sampling(interval)(metricReport(scheduleType, ts9)))
    assert(!sampling(interval)(metricReport(scheduleType, ts10)))
  }

  test("cron bi-secondly") {
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

    val scheduleType = Some(ScheduleType.Cron(Cron.unsafeParse("*/2 * * ? * *")))
    val interval     = 3.second
    assert(!sampling(interval)(metricReport(scheduleType, ts1)))
    assert(sampling(interval)(metricReport(scheduleType, ts2)))
    assert(sampling(interval)(metricReport(scheduleType, ts3)))
    assert(!sampling(interval)(metricReport(scheduleType, ts4)))
    assert(sampling(interval)(metricReport(scheduleType, ts5)))
    assert(sampling(interval)(metricReport(scheduleType, ts6)))
    assert(!sampling(interval)(metricReport(scheduleType, ts7)))
    assert(sampling(interval)(metricReport(scheduleType, ts8)))
    assert(sampling(interval)(metricReport(scheduleType, ts9)))
    assert(!sampling(interval)(metricReport(scheduleType, ts10)))
  }

}
