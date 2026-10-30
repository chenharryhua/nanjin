package com.github.chenharryhua.nanjin.guard.observers.slack

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.logging.{LogLink, LogLocator}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.*
import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.CatsEffectSuite

import scala.concurrent.duration.*
import java.time.Instant

/** Lives in package `com.github.chenharryhua.nanjin.guard.observers.slack` so it can reach the private
  * `SlackTranslator` object and encode the private `SlackApp` it produces. Mirrors `TeamsObserverTest`: a
  * single real service run produces one event of each kind, and each is translated and encoded to JSON so the
  * Block Kit payload can be asserted on. The colors and header emojis pinned here are the wire-visible output
  * `SlackTranslator` sends to Slack.
  */
class SlackTranslatorTest extends CatsEffectSuite {

  private val service = TaskGuard[IO]("slack-test")
    .service("slack-translator-test")

  // A single real service run that produces one event of each kind. Shared across tests via a suite-local
  // fixture so the service is only run once.
  private val allEvents: IO[List[Event]] = service
    .updateConfig(
      _.withReportedEventThreshold(_.Info)
        .withRestartPolicy(1.hour, _.fixedDelay(100.millis).repeat.limited(1)))
    .eventStream { agent =>
      agent.logger.info("info-msg") >>
        agent.logger.warn("warn-msg") >>
        agent.logger.error("error-msg") >>
        agent.adhoc.report >>
        IO.raiseError(new RuntimeException("panic-test"))
    }
    .compile
    .toList

  private val eventsFixture =
    ResourceSuiteLocalFixture("all-events", cats.effect.Resource.eval(allEvents))

  override def munitFixtures = List(eventsFixture)

  private def translate(pf: PartialFunction[Event, Boolean]): IO[Json] = {
    val evt = eventsFixture().find(pf.isDefinedAt).get
    render(evt)
  }

  private def render(event: Event): IO[Json] =
    SlackTranslator[IO].translate(event).map(_.get.asJson)

  test("1.every SlackApp carries the task as username and a non-empty attachments list") {
    translate { case _: ServiceStart => true }.map { json =>
      assert(json.hcursor.get[String]("username").toOption.contains("slack-test"))
      assert(json.hcursor.downField("attachments").values.exists(_.nonEmpty))
    }
  }

  test("2.ServiceStart renders a rocket header with the start title and service name") {
    translate { case _: ServiceStart => true }.map { json =>
      val text = json.noSpaces
      assert(text.contains(":rocket: Start Service"))
      assert(text.contains("slack-translator-test"))
      // ServiceStart has Info level, which colors the attachment blue
      assert(text.contains("#b3d1ff"))
    }
  }

  test("3.ServicePanic renders an alarm header, the exception, and the error color") {
    translate { case _: ServicePanic => true }.map { json =>
      val text = json.noSpaces
      assert(text.contains(":alarm: Service Panic"))
      assert(text.contains("panic-test"))
      assert(text.contains("RuntimeException"))
      assert(text.contains("#935252")) // Error color
    }
  }

  test("4.ServiceStop renders an octagonal-sign header with the stop title") {
    translate { case _: ServiceStop => true }.map { json =>
      val text = json.noSpaces
      assert(text.contains(":octagonal_sign: Stop Service"))
      assert(text.contains("slack-translator-test"))
    }
  }

  test("5.MetricsSnapshot renders the metrics report header") {
    translate { case _: MetricsSnapshot => true }.map { json =>
      val text = json.noSpaces
      assert(text.contains("Metrics Report"))
      assert(text.contains("slack-translator-test"))
    }
  }

  test("6.ReportedEvent Info carries the message, the info color, and no symbol prefix") {
    translate {
      case e: ReportedEvent if e.logRecord.message.value.noSpaces.contains("info-msg") => true
    }.map { json =>
      val text = json.noSpaces
      assert(text.contains("info-msg"))
      assert(text.contains("#b3d1ff")) // Info color
      // Info has no symbol, so the header is just the level title with a leading space
      assert(text.contains(" Info"))
    }
  }

  test("7.ReportedEvent Warn carries a warning symbol and the warn color") {
    translate {
      case e: ReportedEvent if e.logRecord.message.value.noSpaces.contains("warn-msg") => true
    }.map { json =>
      val text = json.noSpaces
      assert(text.contains("warn-msg"))
      assert(text.contains(":warning:"))
      assert(text.contains("#ffd79a")) // Warn color
    }
  }

  test("8.ReportedEvent Error carries a cross symbol and the error color") {
    translate {
      case e: ReportedEvent if e.logRecord.message.value.noSpaces.contains("error-msg") => true
    }.map { json =>
      val text = json.noSpaces
      assert(text.contains("error-msg"))
      assert(text.contains(":x:"))
      assert(text.contains("#935252")) // Error color
    }
  }

  // The log deep-link decorates the brief section's tag on every lifecycle event when present, and is absent
  // when the event carries no `logLink`. It is rendered as Slack's `<url|label>` hyperlink markup.
  private val logUrl = "http://logs.example/find"

  private def withLink(pf: PartialFunction[Event, Boolean]): Event =
    eventsFixture().find(pf.isDefinedAt).get match {
      case e: ServiceStart => e.copy(logLink = Some(LogLink(logUrl)))
      case e: ServicePanic => e.copy(logLink = Some(LogLink(logUrl)))
      case e: ServiceStop  => e.copy(logLink = Some(LogLink(logUrl)))
      case other           => other
    }

  test("9.ServiceStart with a logLink renders the brief tag as a hyperlink") {
    SlackTranslator[IO].translate(withLink { case _: ServiceStart => true }).map { json =>
      assert(json.get.asJson.noSpaces.contains(s"<$logUrl|"))
    }
  }

  test("10.ServicePanic with a logLink hyperlinks the brief tag exactly once") {
    SlackTranslator[IO].translate(withLink { case _: ServicePanic => true }).map { json =>
      val text = json.get.asJson.noSpaces
      val marker = s"<$logUrl|"
      // the panic renders one hyperlink, on the brief section (not the stacktrace/error section)
      assertEquals(text.split(java.util.regex.Pattern.quote(marker), -1).length - 1, 1)
    }
  }

  test("11.a ServiceStart without a logLink emits no hyperlink to the log") {
    translate { case _: ServiceStart => true }.map { json =>
      assert(!json.noSpaces.contains(s"<$logUrl|"))
    }
  }

  test("12.Debug and Good reported events use their color and no-symbol branches") {
    service
      .updateConfig(_.withReportedEventThreshold(_.Debug))
      .eventStream(agent => agent.logger.debug("debug-msg") >> agent.logger.good("good-msg"))
      .compile
      .toList
      .flatMap { events =>
        val debug = events.collectFirst {
          case event: ReportedEvent if event.logRecord.message.value.noSpaces.contains("debug-msg") => event
        }.get
        val good = events.collectFirst {
          case event: ReportedEvent if event.logRecord.message.value.noSpaces.contains("good-msg") => event
        }.get
        for {
          debug_json <- render(debug)
          good_json <- render(good)
        } yield {
          assert(debug_json.noSpaces.contains("#FF00FF"), debug_json.spaces2)
          assert(debug_json.noSpaces.contains("Debug"), debug_json.spaces2)
          assert(good_json.noSpaces.contains("#36a64f"), good_json.spaces2)
          assert(good_json.noSpaces.contains(" Good"), good_json.spaces2)
        }
      }
  }

  test("13.restart ServiceStart renders snooze and non-zero index") {
    val restart = eventsFixture().collect { case event: ServiceStart if event.tick.index > 0L => event }.head

    render(restart).map { json =>
      val text = json.noSpaces
      assert(text.contains("Restart Service"), json.spaces2)
      assert(text.contains("Snooze"), json.spaces2)
      assert(text.contains(restart.tick.index.toString), json.spaces2)
    }
  }

  test("14.homepage links the escaped service name") {
    TaskGuard[IO]("slack-test")
      .service("service<&>")
      .updateConfig(_.withHomepage("https://service.example"))
      .eventStream(_ => IO.unit)
      .collectFirst { case event: ServiceStart => event }
      .compile
      .lastOrError
      .flatMap(render)
      .map { json =>
        val text = json.noSpaces
        assert(text.contains("<https://service.example|service&lt;&amp;&gt;>"), json.spaces2)
      }
  }

  test("15.non-empty metrics render YAML and hyperlink the snapshot tag") {
    val locator = new LogLocator {
      override def locate(timestamp: Instant): LogLink =
        LogLink(s"https://logs.example/${timestamp.toEpochMilli}")
    }

    service
      .updateConfig(_.withLogLocator(IO.pure(Some(locator))))
      .eventStream { agent =>
        agent.facilitate("metrics")(_.counter("counter")).use(_.inc(2) >> agent.adhoc.report)
      }
      .collectFirst { case event: MetricsSnapshot => event }
      .compile
      .lastOrError
      .flatMap(render)
      .map { json =>
        val text = json.noSpaces
        assert(text.contains("counter"), json.spaces2)
        assert(text.contains("https://logs.example"), json.spaces2)
        assert(!text.contains("not available"), json.spaces2)
      }
  }

  test("16.reported throwable renders a stack attachment and log-link tag") {
    val locator = new LogLocator {
      override def locate(timestamp: Instant): LogLink =
        LogLink(s"https://logs.example/${timestamp.toEpochMilli}")
    }

    service
      .updateConfig(_.withLogLocator(IO.pure(Some(locator)))
        .withReportedEventThreshold(_.Info))
      .eventStream(agent => agent.logger.error("message", new RuntimeException("reported-stack")))
      .collectFirst { case event: ReportedEvent => event }
      .compile
      .lastOrError
      .flatMap(render)
      .map { json =>
        val text = json.noSpaces
        assert(text.contains("reported-stack"), json.spaces2)
        assert(text.contains("StackTrace"), json.spaces2)
        assert(text.contains("https://logs.example"), json.spaces2)
      }
  }

  test("17.long messages are abbreviated below Slack's block limit") {
    val original = "x" * 4000

    service
      .updateConfig(_.withReportedEventThreshold(_.Info))
      .eventStream(_.logger.info(original))
      .collectFirst { case event: ReportedEvent => event }
      .compile
      .lastOrError
      .flatMap(render)
      .map { json =>
        val text = json.noSpaces
        assert(!text.contains(original), "full 4000-character message was not abbreviated")
        assert(text.contains("..."), json.spaces2)
      }
  }
}
