package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.utils.zzffEpoch
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.{MetricParams, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.observers.sampling
import cron4s.Cron
import cron4s.expr.CronExpr
import eu.timepit.refined.auto.*
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZonedDateTime
import scala.concurrent.duration.*

class MetricSamplingTest extends AnyFunSuite {
  val launchTime: ZonedDateTime = ZonedDateTime.of(zzffEpoch, beijingTime)

  val serviceParams: ServiceParams =
    ServiceParams.launchTime.set(launchTime)(
      TaskGuard[IO]("test").service("sampling").dummyAgent.use(a => IO(a.serviceParams)).unsafeRunSync())

  def metricReport(cron: CronExpr, now: ZonedDateTime): MetricReport = MetricReport(
    MetricIndex.Periodic(1023),
    ServiceParams.metricParams.composeLens(MetricParams.reportSchedule).set(Some(cron))(serviceParams),
    now,
    MetricSnapshot(Map.empty[String, Long], Json.Null, "")
  )

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

    val interval = 3.second
    assert(!sampling(interval)(metricReport(secondly, ts1)))
    assert(!sampling(interval)(metricReport(secondly, ts2)))
    assert(sampling(interval)(metricReport(secondly, ts3)))
    assert(!sampling(interval)(metricReport(secondly, ts4)))
    assert(!sampling(interval)(metricReport(secondly, ts5)))
    assert(sampling(interval)(metricReport(secondly, ts6)))
    assert(!sampling(interval)(metricReport(secondly, ts7)))
    assert(!sampling(interval)(metricReport(secondly, ts8)))
    assert(sampling(interval)(metricReport(secondly, ts9)))
    assert(!sampling(interval)(metricReport(secondly, ts10)))
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

    val interval = 3.second
    assert(!sampling(interval)(metricReport(bisecondly, ts1)))
    assert(sampling(interval)(metricReport(bisecondly, ts2)))
    assert(sampling(interval)(metricReport(bisecondly, ts3)))
    assert(!sampling(interval)(metricReport(bisecondly, ts4)))
    assert(sampling(interval)(metricReport(bisecondly, ts5)))
    assert(sampling(interval)(metricReport(bisecondly, ts6)))
    assert(!sampling(interval)(metricReport(bisecondly, ts7)))
    assert(sampling(interval)(metricReport(bisecondly, ts8)))
    assert(sampling(interval)(metricReport(bisecondly, ts9)))
    assert(!sampling(interval)(metricReport(bisecondly, ts10)))
  }

  test("cron sampling - cron") {
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
    assert(!sampling(hourly)(metricReport(minutely, ts1)))
    assert(!sampling(hourly)(metricReport(minutely, ts2)))
    assert(sampling(hourly)(metricReport(minutely, ts3)))
    assert(sampling(hourly)(metricReport(minutely, ts4)))
    assert(!sampling(hourly)(metricReport(minutely, ts5)))
    assert(!sampling(hourly)(metricReport(minutely, ts6)))
    assert(!sampling(hourly)(metricReport(minutely, ts7)))
    assert(!sampling(hourly)(metricReport(minutely, ts8)))
    assert(!sampling(hourly)(metricReport(minutely, ts9)))
  }
}
