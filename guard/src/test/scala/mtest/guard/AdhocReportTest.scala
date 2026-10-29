package mtest.guard

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.logging.{LogLink, LogLocator}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.{
  MetricsSnapshot,
  ReportedEvent,
  ServicePanic,
  ServiceStart,
  ServiceStop
}
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.ScrapeMode
import io.circe.Json
import munit.CatsEffectSuite

import java.time.Instant
import scala.concurrent.duration.DurationInt

class AdhocReportTest extends CatsEffectSuite {

  private val service = TaskGuard[IO]("adhoc-report").service("adhoc-report")

  test("1.snapshots defaults to Cheap mode and excludes gauges") {
    service
      .eventStream { agent =>
        agent.facilitate("cheap")(_.gauge("gauge", _.register(IO.pure(Json.fromInt(1))))).use { _ =>
          agent.adhoc
            .snapshots(_.fixedDelay(10.millis).repeat)
            .take(1)
            .compile
            .lastOrError
            .map(snapshot => assertEquals(snapshot.snapshot.gauges, Nil))
        }
      }
      .compile
      .drain
  }

  test("2.snapshots accepts Full mode and includes gauges") {
    service
      .eventStream { agent =>
        agent.facilitate("full")(_.gauge("gauge", _.register(IO.pure(Json.fromInt(1))))).use { _ =>
          agent.adhoc
            .snapshots(_.fixedDelay(10.millis).repeat, _ => ScrapeMode.Full)
            .take(1)
            .compile
            .lastOrError
            .map(snapshot => assertEquals(snapshot.snapshot.gauges.size, 1))
        }
      }
      .compile
      .drain
  }

  test("3.meteredCounts emits the initial count followed by a delta") {
    service
      .eventStream { agent =>
        agent.facilitate("metered")(_.meter("meter")).use { meter =>
          meter.mark(5) >> agent.adhoc
            .meteredCounts(_.fixedDelay(20.millis).repeat)
            .zipWithIndex
            .evalTap { case (_, index) => if index == 0L then meter.mark(3) else IO.unit }
            .take(2)
            .compile
            .toList
            .map { counts =>
              assertEquals(counts.map(_._1.counts.values.head), List(5L, 3L))
            }
        }
      }
      .compile
      .drain
  }

  test("4.configured LogLocator is applied to service, reported, and metric events") {
    val locator = new LogLocator {
      override def locate(timestamp: Instant): LogLink = LogLink(s"logs:${timestamp.toEpochMilli}")
    }

    service
      .updateConfig(
        _.withLogLocator(IO.pure(Some(locator)))
          .withReportedEventThreshold(_.Info))
      .eventStream(agent => agent.logger.error("failure") >> agent.adhoc.report)
      .map(checkJson)
      .compile
      .toList
      .map { events =>
        val links = events.collect {
          case event: ServiceStart    => event.logLink
          case event: ReportedEvent   => event.logLink
          case event: MetricsSnapshot => event.logLink
          case event: ServiceStop     => event.logLink
        }
        assertEquals(links.size, 4)
        assert(links.forall(_.exists(_.value.startsWith("logs:"))))
      }
  }

  test("5.configured LogLocator is applied to panic events") {
    val locator = new LogLocator {
      override def locate(timestamp: Instant): LogLink = LogLink(s"panic:${timestamp.toEpochMilli}")
    }

    service
      .updateConfig(_.withLogLocator(IO.pure(Some(locator)))
        .withRestartPolicy(1.hour, _.fixedDelay(10.millis).repeat.limited(1)))
      .eventStream(_ => IO.raiseError(new RuntimeException("panic")))
      .map(checkJson)
      .collect { case event: ServicePanic => event }
      .compile
      .toList
      .map { panics =>
        assertEquals(panics.size, 1)
        assert(panics.head.logLink.exists(_.value.startsWith("panic:")))
      }
  }
}
