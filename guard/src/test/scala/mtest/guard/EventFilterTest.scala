package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.chrono.{crontabs, tickStream, Policy}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event.{MetricsSnapshot, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import scala.concurrent.duration.DurationInt
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index.Periodic
import com.github.chenharryhua.nanjin.guard.event.EventPipe

class EventFilterTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("event.filters").service("filters").updateConfig(_.withLogFormat(_.Slf4j_Json_OneLine))

  test("1.sampling - FiniteDuration") {
    val List(a, b, c, d) = service
      .updateConfig(_.withMetricReport(_.crontab(_.secondly)))
      .eventStream(_ => IO.sleep(7.seconds))
      .map(checkJson)
      .filter(EventPipe.windowFilter(3.seconds).filter)
      .compile
      .toList
      .unsafeRunSync()
    val first = b.asInstanceOf[MetricsSnapshot].index.asInstanceOf[Periodic].tick.index
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[MetricsSnapshot])
    assert(c.asInstanceOf[MetricsSnapshot].index.asInstanceOf[Periodic].tick.index === first + 3)
    assert(d.isInstanceOf[ServiceStop])
  }

  test("2.sampling - divisor") {
    val List(a, b, c, d) = service
      .updateConfig(_.withMetricReport(_.crontab(_.secondly)))
      .eventStream(_ => IO.sleep(7.seconds))
      .map(checkJson)
      .filter(EventPipe.indexFilter(3).filter)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[MetricsSnapshot].index.asInstanceOf[Periodic].tick.index === 3)
    assert(c.asInstanceOf[MetricsSnapshot].index.asInstanceOf[Periodic].tick.index === 6)
    assert(d.isInstanceOf[ServiceStop])
  }

  test("3.sampling - cron") {
    val policy = Policy.crontab(_.secondly)
    val align = tickStream.tickScheduled[IO](sydneyTime, _.crontab(_.every3Seconds).limited(1))
    val run = service
      .updateConfig(_.withMetricReport(_ => policy))
      .eventStream(_ => IO.sleep(7.seconds))
      .map(checkJson)
      .filter(EventPipe.cronFilter(crontabs.every3Seconds).filter)

    val List(a, b, c, d) = align.flatMap(_ => run).compile.toList.unsafeRunSync()
    val tb = b.asInstanceOf[MetricsSnapshot].index.asInstanceOf[Periodic].tick
    val tc = c.asInstanceOf[MetricsSnapshot].index.asInstanceOf[Periodic].tick
    assert(a.isInstanceOf[ServiceStart])
    assert(tb.index + 3 == tc.index)
    assert(Duration.between(tb.conclude, tc.conclude) == Duration.ofSeconds(3))
    assert(d.isInstanceOf[ServiceStop])
  }
}
