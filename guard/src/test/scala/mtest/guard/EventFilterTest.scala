package mtest.guard

import cats.data.NonEmptyList
import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.chrono.{crontabs, tickStream, Policy}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event.{MetricsSnapshot, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import munit.CatsEffectSuite

import java.time.{Duration, LocalTime}
import scala.concurrent.duration.DurationInt
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot.Periodic
import com.github.chenharryhua.nanjin.guard.event.{Event, EventPipe}
import fs2.Stream

class EventFilterTest extends CatsEffectSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("event.filters").service("filters")

  test("1.sampling - FiniteDuration") {
    service
      .updateConfig(_.withReportPolicy(_.crontab(_.secondly).repeat))
      .eventStream(_ => IO.sleep(7.seconds))
      .map(checkJson)
      .filter(EventPipe.windowFilter(3.seconds).filter)
      .compile
      .toList
      .map { events =>
        val List(a, b, c, d) = events: @unchecked
        val first = b.asInstanceOf[MetricsSnapshot].index.asInstanceOf[Periodic].tick.index
        assert(a.isInstanceOf[ServiceStart])
        assert(b.isInstanceOf[MetricsSnapshot])
        assert(c.asInstanceOf[MetricsSnapshot].index.asInstanceOf[Periodic].tick.index == first + 3)
        assert(d.isInstanceOf[ServiceStop])
      }
  }

  test("2.sampling - divisor") {
    service
      .updateConfig(_.withReportPolicy(_.crontab(_.secondly).repeat))
      .eventStream(_ => IO.sleep(7.seconds))
      .map(checkJson)
      .filter(EventPipe.indexFilter(3).filter)
      .compile
      .toList
      .map { events =>
        val List(a, b, c, d) = events: @unchecked
        assert(a.isInstanceOf[ServiceStart])
        assert(b.asInstanceOf[MetricsSnapshot].index.asInstanceOf[Periodic].tick.index == 3)
        assert(c.asInstanceOf[MetricsSnapshot].index.asInstanceOf[Periodic].tick.index == 6)
        assert(d.isInstanceOf[ServiceStop])
      }
  }

  test("3.sampling - cron") {
    val policy = Policy.crontab(_.secondly).repeat
    val align = tickStream.tickScheduled[IO](sydneyTime, _.crontab(_.every3Seconds))
    val run = service
      .updateConfig(_.withReportPolicy(_ => policy))
      .eventStream(_ => IO.sleep(7.seconds))
      .map(checkJson)
      .filter(EventPipe.cronFilter(crontabs.every3Seconds).filter)

    align.flatMap(_ => run).compile.toList.map { events =>
      val List(a, b, c, d) = events: @unchecked
      val tb = b.asInstanceOf[MetricsSnapshot].index.asInstanceOf[Periodic].tick
      val tc = c.asInstanceOf[MetricsSnapshot].index.asInstanceOf[Periodic].tick
      assert(a.isInstanceOf[ServiceStart])
      assert(tb.index + 3 == tc.index)
      assert(Duration.between(tb.conclude, tc.conclude) == Duration.ofSeconds(3))
      assert(d.isInstanceOf[ServiceStop])
    }
  }

  test("4.sampling - local time") {
    val run: Stream[IO, Event] = service
      .updateConfig(_.withReportPolicy(_.crontab(_.secondly).repeat))
      .eventStream(_ => IO.sleep(7.seconds))
      .map(checkJson)
      .filter(
        EventPipe.localTimeFilter(
          NonEmptyList.of(
            LocalTime.now().plusSeconds(3),
            LocalTime.now().plusSeconds(12),
            LocalTime.now().plusSeconds(120))
        ).filter)

    run.compile.toList.map { events =>
      val List(a, b, c) = events: @unchecked
      assert(b.asInstanceOf[MetricsSnapshot].index.isInstanceOf[Periodic])
      assert(a.isInstanceOf[ServiceStart])
      assert(c.isInstanceOf[ServiceStop])
    }
  }
}
