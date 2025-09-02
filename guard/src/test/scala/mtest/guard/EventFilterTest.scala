package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event.{MetricReport, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.MetricIndex.Periodic
import com.github.chenharryhua.nanjin.guard.event.eventFilters
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import scala.concurrent.duration.DurationInt

class EventFilterTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("event.filters").service("filters").updateConfig(_.withLogFormat(_.Slf4j_Json_OneLine))

  test("1.sampling - FiniteDuration") {
    val List(a, b, c, d) = service
      .updateConfig(_.withMetricReport(_.crontab(_.secondly)))
      .eventStream(_ => IO.sleep(7.seconds))
      .map(checkJson)
      .filter(eventFilters.sampling(3.seconds))
      .compile
      .toList
      .unsafeRunSync()
    val first = b.asInstanceOf[MetricReport].index.asInstanceOf[Periodic].tick.index
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[MetricReport])
    assert(c.asInstanceOf[MetricReport].index.asInstanceOf[Periodic].tick.index === first + 3)
    assert(d.isInstanceOf[ServiceStop])
  }

  test("2.sampling - divisor") {
    val List(a, b, c, d) = service
      .updateConfig(_.withMetricReport(_.crontab(_.secondly)))
      .eventStream(_ => IO.sleep(7.seconds))
      .map(checkJson)
      .filter(eventFilters.sampling(3))
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[MetricReport].index.asInstanceOf[Periodic].tick.index === 3)
    assert(c.asInstanceOf[MetricReport].index.asInstanceOf[Periodic].tick.index === 6)
    assert(d.isInstanceOf[ServiceStop])
  }

  test("3.sampling - cron") {
    val policy = Policy.crontab(_.secondly)
    val align = tickStream.tickPast[IO](sydneyTime, Policy.crontab(_.every3Seconds).limited(1))
    val run = service
      .updateConfig(_.withMetricReport(1, policy))
      .eventStream(_ => IO.sleep(7.seconds))
      .map(checkJson)
      .filter(eventFilters.sampling(_.every3Seconds))

    val List(a, b, c, d) = align.flatMap(_ => run).compile.toList.unsafeRunSync()
    val tb = b.asInstanceOf[MetricReport].index.asInstanceOf[Periodic].tick
    val tc = c.asInstanceOf[MetricReport].index.asInstanceOf[Periodic].tick
    assert(a.isInstanceOf[ServiceStart])
    assert(tb.index + 3 == tc.index)
    assert(Duration.between(tb.conclude, tc.conclude) == Duration.ofSeconds(3))
    assert(d.isInstanceOf[ServiceStop])
  }
}
