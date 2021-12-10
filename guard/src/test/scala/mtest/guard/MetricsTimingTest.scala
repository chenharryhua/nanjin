package mtest.guard

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.utils.zzffEpoch
import com.github.chenharryhua.nanjin.datetime.beijingTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZonedDateTime
import scala.concurrent.duration.DurationInt

class MetricsTimingTest extends AnyFunSuite {
  val launchTime = ZonedDateTime.of(zzffEpoch, beijingTime)
  val now        = ZonedDateTime.of(2021, 1, 1, 0, 0, 0, 0, beijingTime)

  val service = TaskGuard[IO]("metrics").service("timing")

  test("should return none when no report is scheduled") {
    assert(service.params.metric.next(now, None, launchTime).isEmpty)
  }

  test("cron secondly schedule without interval") {
    val m = service.updateConfig(_.withMetricSchedule("* * * ? * *")).params.metric
    assert(m.next(now, None, launchTime).get == now.plusSeconds(1))
    assert(m.isShow(now, None, launchTime))
  }

  test("cron minutely schedule without interval") {
    val m = service.updateConfig(_.withMetricSchedule("0 * * ? * *")).params.metric
    assert(m.next(now, None, launchTime).get == now.plusMinutes(1))
    assert(m.isShow(now, None, launchTime))
  }

  test("cron hourly schedule without interval") {
    val m = service.updateConfig(_.withMetricSchedule("0 0 * ? * *")).params.metric
    assert(m.next(now, None, launchTime).get == now.plusHours(1))
    assert(m.isShow(now, None, launchTime))
  }

  test("fixed rate secondly") {
    val m = service.updateConfig(_.withMetricSchedule(1.second)).params.metric
    assert(m.next(now, None, launchTime).get == now.plusSeconds(1))
    assert(m.isShow(now, None, launchTime))
  }

  test("fixed rate minutely") {
    val m = service.updateConfig(_.withMetricSchedule(1.minute)).params.metric
    assert(m.next(now, None, launchTime).get == now.plusMinutes(1))
    assert(m.isShow(now, None, launchTime))
  }

  test("fixed rate hourly") {
    val m = service.updateConfig(_.withMetricSchedule(1.hour)).params.metric
    assert(m.next(now, None, launchTime).get == now.plusHours(1))
    assert(m.isShow(now, None, launchTime))
  }

  test("fixed rate + interval - 1") {
    val interval = Some(1.minute)
    val m        = service.updateConfig(_.withMetricSchedule(5.second)).params.metric
    assert(m.next(now, interval, launchTime).get == now.plusMinutes(1))
    assert(m.isShow(now, interval, launchTime))
  }

  test("fixed rate + interval - 2") {
    val now2     = now.plusSeconds(10)
    val interval = Some(1.minute)
    val m        = service.updateConfig(_.withMetricSchedule(5.second)).params.metric
    assert(m.next(now2, interval, launchTime).get == now.plusMinutes(1))
    assert(!m.isShow(now2, interval, launchTime))
  }

  test("cron + interval - 1") {
    val interval = Some(1.minute)
    val m        = service.updateConfig(_.withMetricSchedule("* * * ? * *")).params.metric
    assert(m.next(now, interval, launchTime).get == now.plusMinutes(1))
    assert(m.isShow(now, interval, launchTime))
  }

  test("cron + interval - 2") {
    val now2     = now.plusSeconds(10)
    val interval = Some(1.minute)
    val m        = service.updateConfig(_.withMetricSchedule("* * * ? * *")).params.metric
    assert(m.next(now2, interval, launchTime).get == now.plusMinutes(1))
    assert(!m.isShow(now2, interval, launchTime))
  }
}
