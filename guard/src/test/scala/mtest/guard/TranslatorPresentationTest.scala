package mtest.guard

import cats.Eval
import cats.effect.IO
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.StackTrace
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.{
  MetricsSnapshot,
  ReportedEvent,
  ServicePanic,
  ServiceStart,
  ServiceStop
}
import com.github.chenharryhua.nanjin.guard.event.StopReason
import com.github.chenharryhua.nanjin.guard.translator.{
  eventLogLevel,
  eventTitle,
  htmlColoring,
  panicText,
  AnsiTextTranslator
}
import munit.CatsEffectSuite

import java.time.Duration
import scala.concurrent.duration.DurationInt

class TranslatorPresentationTest extends CatsEffectSuite {

  private val service = TaskGuard[IO]("translator-presentation").service("translator-presentation")

  private def level(event: Event): LogLevel =
    eventLogLevel[Eval, LogLevel](event).run(value => Eval.now(value)).value

  private def render(event: Event): IO[String] =
    AnsiTextTranslator[IO].translate(event).map(_.getOrElse(fail(s"event was not rendered: $event")))

  test("1.event titles, levels, and HTML colors cover starts, stops, and every reported level") {
    service
      .updateConfig(_.withReportedEventThreshold(_.Debug))
      .eventStream { agent =>
        agent.logger.debug("debug") >>
          agent.logger.info("info") >>
          agent.logger.good("good") >>
          agent.logger.warn("warn") >>
          agent.logger.error("error")
      }
      .map(checkJson)
      .compile
      .toList
      .map { events =>
        val start = events.collectFirst { case event: ServiceStart => event }.get
        val stop = events.collectFirst { case event: ServiceStop => event }.get
        val reported = events.collect { case event: ReportedEvent => event }

        assertEquals(eventTitle(start), "Start Service")
        assertEquals(eventTitle(start.copy(tick = start.tick.copy(index = 1L))), "Restart Service")
        assertEquals(level(start), LogLevel.Info)
        assertEquals(htmlColoring(start), "color:black")
        assertEquals(eventTitle(stop), "Stop Service")
        assertEquals(level(stop), LogLevel.Good)
        assertEquals(htmlColoring(stop), "color:darkgreen")
        assertEquals(reported.map(eventTitle).toSet, LogLevel.values.map(_.productPrefix).toSet)
        assertEquals(reported.map(level).toSet, LogLevel.values.toSet)
        assert(reported.forall(event => htmlColoring(event).nonEmpty))
      }
  }

  test("2.metric snapshot severity distinguishes empty, healthy, risky, and unhealthy snapshots") {
    service
      .eventStream { agent =>
        val healthy = agent.facilitate("healthy")(_.counter("counter")).use { counter =>
          counter.inc(1) >> agent.adhoc.report
        }
        val unhealthy = agent.facilitate("unhealthy") { hub =>
          for {
            risk <- hub.counter("risk", _.asRisk)
            _ <- hub.healthCheck("health", _.register(IO.pure(false)))
          } yield risk
        }.use(risk => risk.inc(1) >> agent.adhoc.report)

        agent.adhoc.report >> healthy >> unhealthy
      }
      .map(checkJson)
      .collect { case event: MetricsSnapshot => event }
      .compile
      .toList
      .flatMap { snapshots =>
        IO {
          assertEquals(snapshots.size, 3)
          assertEquals(snapshots.map(level), List(LogLevel.Warn, LogLevel.Info, LogLevel.Error))
          assertEquals(eventTitle(snapshots.head), "Metrics Report")
          assertEquals(htmlColoring(snapshots.head), "color:#b3b300")
          assertEquals(htmlColoring(snapshots.last), "color:red")
        } >> render(snapshots(1)).map { text =>
          assert(text.contains("Metrics Report"), text)
          assert(text.contains("counter"), text)
        }
      }
  }

  test("3.panic title, text, color, and ANSI rendering include restart and stack-trace details") {
    val marker = "translator-panic-marker"
    val panic = service
      .updateConfig(_.withRestartPolicy(1.hour, _.fixedDelay(10.millis).repeat.limited(1)))
      .eventStream(_ => IO.raiseError(new RuntimeException(marker)))
      .map(checkJson)
      .collect { case event: ServicePanic => event }
      .take(1)
      .compile
      .lastOrError

    panic.flatMap { event =>
      render(event).map { text =>
        assertEquals(eventTitle(event), "Service Panic")
        assertEquals(level(event), LogLevel.Error)
        assertEquals(htmlColoring(event), "color:red")
        assert(panicText(event).startsWith("Restart scheduled"))
        assert(text.contains(marker), text)
        assert(text.contains("Service Panic"), text)
      }
    }
  }

  test("4.panicText uses a date-time for restart delays of at least 24 hours") {
    val panic = service
      .updateConfig(_.withRestartPolicy(2.days, _.fixedDelay(25.hours).repeat.limited(1)))
      .eventStream(_ => IO.raiseError(new RuntimeException("long-delay")))
      .map(checkJson)
      .collect { case event: ServicePanic => event }
      .take(1)
      .compile
      .lastOrError

    panic.map { event =>
      val stretched = event.copy(tick = event.tick.withSnoozeStretch(Duration.ofHours(25)))
      val text = panicText(stretched)
      assert(text.contains("T"), text)
      assert(text.contains("day"), text)
    }
  }

  test("5.every stop reason maps to its severity and renders exception detail only when present") {
    service
      .eventStream(_ => IO.unit)
      .map(checkJson)
      .collect { case event: ServiceStop => event }
      .compile
      .lastOrError
      .flatMap { successful =>
        val cancellation = successful.copy(cause = StopReason.ByCancellation)
        val maintenance = successful.copy(cause = StopReason.Maintenance)
        val exception = successful.copy(
          cause = StopReason.ByException(StackTrace(new RuntimeException("stop-exception-marker"))))

        for {
          successful_text <- render(successful)
          cancellation_text <- render(cancellation)
          maintenance_text <- render(maintenance)
          exception_text <- render(exception)
        } yield {
          assertEquals(level(successful), LogLevel.Good)
          assertEquals(level(cancellation), LogLevel.Warn)
          assertEquals(level(maintenance), LogLevel.Info)
          assertEquals(level(exception), LogLevel.Error)
          assert(!successful_text.contains("StackTrace:"))
          assert(!cancellation_text.contains("StackTrace:"))
          assert(!maintenance_text.contains("StackTrace:"))
          assert(exception_text.contains("stop-exception-marker"), exception_text)
        }
      }
  }

  test("6.reported events with a cause render message and stack trace") {
    val marker = "reported-stack-marker"
    service
      .eventStream(agent => agent.logger.error("reported-message", new RuntimeException(marker)))
      .map(checkJson)
      .collect { case event: ReportedEvent => event }
      .compile
      .lastOrError
      .flatMap(render)
      .map { text =>
        assert(text.contains("reported-message"), text)
        assert(text.contains(marker), text)
      }
  }
}
