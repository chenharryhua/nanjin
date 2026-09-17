package com.github.chenharryhua.nanjin.guard.observers.slack

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.*
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

/** Lives in package `com.github.chenharryhua.nanjin.guard.observers.slack` so it can reach the private
  * `SlackTranslator` object and encode the private `SlackApp` it produces. Mirrors `TeamsObserverTest`: a
  * single real service run produces one event of each kind, and each is translated and encoded to JSON so the
  * Block Kit payload can be asserted on. The colors and header emojis pinned here are the wire-visible output
  * `SlackTranslator` sends to Slack.
  */
class SlackTranslatorTest extends AnyFunSuite {

  private val service = TaskGuard[IO]("slack-test")
    .service("slack-translator-test")

  private lazy val allEvents: List[Event] = service
    .updateConfig(
      _.withLogThreshold(_.Info, _.Info)
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
    .unsafeRunSync()

  private def translate(pf: PartialFunction[Event, Boolean]): Json = {
    val translator = SlackTranslator[IO]
    val evt = allEvents.find(pf.isDefinedAt).get
    translator.translate(evt).unsafeRunSync().get.asJson
  }

  test("1.every SlackApp carries the task as username and a non-empty attachments list") {
    val json = translate { case _: ServiceStart => true }
    assert(json.hcursor.get[String]("username").toOption.contains("slack-test"))
    assert(json.hcursor.downField("attachments").values.exists(_.nonEmpty))
  }

  test("2.ServiceStart renders a rocket header with the start title and service name") {
    val text = translate { case _: ServiceStart => true }.noSpaces
    assert(text.contains(":rocket: Start Service"))
    assert(text.contains("slack-translator-test"))
    // ServiceStart is logged at Info level, which colors the attachment blue
    assert(text.contains("#b3d1ff"))
  }

  test("3.ServicePanic renders an alarm header, the exception, and the error color") {
    val text = translate { case _: ServicePanic => true }.noSpaces
    assert(text.contains(":alarm: Service Panic"))
    assert(text.contains("panic-test"))
    assert(text.contains("RuntimeException"))
    assert(text.contains("#935252")) // Error color
  }

  test("4.ServiceStop renders an octagonal-sign header with the stop title") {
    val text = translate { case _: ServiceStop => true }.noSpaces
    assert(text.contains(":octagonal_sign: Stop Service"))
    assert(text.contains("slack-translator-test"))
  }

  test("5.MetricsSnapshot renders the metrics report header") {
    val text = translate { case _: MetricsSnapshot => true }.noSpaces
    assert(text.contains("Metrics Report"))
    assert(text.contains("slack-translator-test"))
  }

  test("6.ReportedEvent Info carries the message, the info color, and no symbol prefix") {
    val text = translate {
      case e: ReportedEvent if e.message.value.noSpaces.contains("info-msg") => true
    }.noSpaces
    assert(text.contains("info-msg"))
    assert(text.contains("#b3d1ff")) // Info color
    // Info has no symbol, so the header is just the level title with a leading space
    assert(text.contains(" Info"))
  }

  test("7.ReportedEvent Warn carries a warning symbol and the warn color") {
    val text = translate {
      case e: ReportedEvent if e.message.value.noSpaces.contains("warn-msg") => true
    }.noSpaces
    assert(text.contains("warn-msg"))
    assert(text.contains(":warning:"))
    assert(text.contains("#ffd79a")) // Warn color
  }

  test("8.ReportedEvent Error carries a cross symbol and the error color") {
    val text = translate {
      case e: ReportedEvent if e.message.value.noSpaces.contains("error-msg") => true
    }.noSpaces
    assert(text.contains("error-msg"))
    assert(text.contains(":x:"))
    assert(text.contains("#935252")) // Error color
  }
}
