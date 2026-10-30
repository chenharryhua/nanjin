package com.github.chenharryhua.nanjin.guard.observers.teams

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.common.logging.{LogLink, LogLocator}
import com.github.chenharryhua.nanjin.guard.event.Event.*
import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.circe.CirceEntityDecoder.*
import org.http4s.client.Client
import org.http4s.dsl.io.*
import org.http4s.implicits.*

import scala.concurrent.duration.*
import java.time.Instant

class TeamsObserverTest extends CatsEffectSuite {

  private val service = TaskGuard[IO]("teams-test")
    .service("teams-observer-test")
    .updateConfig(_.withRestartPolicy(1.hour, _.fixedDelay(100.millis).repeat.limited(1)))

  private def mockClient(received: Ref[IO, List[Json]]): Client[IO] =
    Client.fromHttpApp(HttpApp[IO] { req =>
      req.as[Json].flatMap(json => received.update(_ :+ json) *> Ok("1"))
    })

  // --- Observer integration tests ---

  test("1.TeamsObserver posts cards to webhook for each event") {
    val received = Ref.unsafe[IO, List[Json]](Nil)
    val client = Resource.pure[IO, Client[IO]](mockClient(received))
    val observer = TeamsObserver[IO](client)

    for {
      events <- service
        .eventStream(_ => IO.unit)
        .through(observer.observe(uri"http://localhost/webhook"))
        .compile
        .toList
      posted <- received.get
    } yield {
      assert(events.exists(_.isInstanceOf[ServiceStart]))
      assert(events.exists(_.isInstanceOf[ServiceStop]))
      assert(posted.size == events.size)
    }
  }

  test("2.TeamsObserver survives webhook failure without dropping events") {
    val failClient = Client.fromHttpApp(HttpApp[IO](_ => InternalServerError("boom")))
    val client = Resource.pure[IO, Client[IO]](failClient)
    val observer = TeamsObserver[IO](client)

    service
      .eventStream(_ => IO.unit)
      .through(observer.observe(uri"http://localhost/webhook"))
      .compile
      .toList
      .map { events =>
        assert(events.exists(_.isInstanceOf[ServiceStart]))
        assert(events.exists(_.isInstanceOf[ServiceStop]))
      }
  }

  test("3.withTranslator allows skipping event types") {
    val received = Ref.unsafe[IO, List[Json]](Nil)
    val client = Resource.pure[IO, Client[IO]](mockClient(received))
    val observer = TeamsObserver[IO](client).withTranslator(_.skipMetricsSnapshot)

    for {
      events <- service
        .eventStream(agent => agent.adhoc.report)
        .through(observer.observe(uri"http://localhost/webhook"))
        .compile
        .toList
      posted <- received.get
    } yield {
      assert(events.exists(_.isInstanceOf[MetricsSnapshot]))
      assert(posted.size < events.size)
    }
  }

  // --- Per-event card content tests ---

  private val allEvents: IO[List[com.github.chenharryhua.nanjin.guard.event.Event]] = service
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
    ResourceSuiteLocalFixture("all-events", Resource.eval(allEvents))

  override def munitFixtures = List(eventsFixture)

  private def translateEvent(
    pf: PartialFunction[com.github.chenharryhua.nanjin.guard.event.Event, Boolean]): IO[Json] = {
    val translator = TeamsTranslator[IO]
    val evt = eventsFixture().find(pf.isDefinedAt).get
    translator.translate(evt).map(card => summon[io.circe.Encoder[AdaptiveCard]].apply(card.get))
  }

  test("4.ServiceStart card contains service name and index") {
    translateEvent { case _: ServiceStart => true }.map { json =>
      val text = json.noSpaces
      assert(text.contains("teams-observer-test"))
      assert(text.contains("Index"))
      assert(text.contains("Start Service"))
    }
  }

  test("5.ServicePanic card contains stack trace and exception message") {
    translateEvent { case _: ServicePanic => true }.map { json =>
      val text = json.noSpaces
      assert(text.contains("panic-test"))
      assert(text.contains("RuntimeException"))
      assert(text.contains("Service Panic"))
    }
  }

  test("6.ServiceStop card contains stop reason") {
    translateEvent { case _: ServiceStop => true }.map { json =>
      val text = json.noSpaces
      assert(text.contains("Stop Service"))
      assert(text.contains("teams-observer-test"))
    }
  }

  test("7.MetricsSnapshot card contains snapshot data") {
    translateEvent { case _: MetricsSnapshot => true }.map { json =>
      val text = json.noSpaces
      assert(text.contains("Metrics Report"))
      assert(text.contains("teams-observer-test"))
    }
  }

  test("8.ReportedEvent Info card contains correlation and message") {
    translateEvent {
      case e: ReportedEvent if e.logRecord.message.value.noSpaces.contains("info-msg") => true
    }.map { json =>
      val text = json.noSpaces
      assert(text.contains("info-msg"))
      assert(text.contains("Correlation"))
      assert(text.contains("Info"))
    }
  }

  test("9.ReportedEvent Warn card has warning color") {
    translateEvent {
      case e: ReportedEvent if e.logRecord.message.value.noSpaces.contains("warn-msg") => true
    }.map { json =>
      val text = json.noSpaces
      assert(text.contains("warn-msg"))
      assert(text.contains("Warning"))
    }
  }

  test("10.ReportedEvent Error card has error color") {
    translateEvent {
      case e: ReportedEvent if e.logRecord.message.value.noSpaces.contains("error-msg") => true
    }.map { json =>
      val text = json.noSpaces
      assert(text.contains("error-msg"))
      assert(text.contains("Attention"))
    }
  }

  test("11.observer POSTs to the supplied URI without an Idempotency-Key header") {
    final case class Captured(method: Method, uri: Uri, hasIdempotencyKey: Boolean, body: Json)

    for {
      captured <- Ref.of[IO, List[Captured]](Nil)
      http = Client.fromHttpApp(HttpApp[IO] { request =>
        request.as[Json].flatMap { body =>
          val has_key = request.headers.headers.exists(_.name.toString.equalsIgnoreCase("Idempotency-Key"))
          captured.update(_ :+ Captured(request.method, request.uri, has_key, body)) *> Ok("accepted")
        }
      })
      observer = TeamsObserver[IO](Resource.pure(http))
      _ <- service
        .eventStream(_ => IO.unit)
        .through(observer.observe(uri"http://localhost/custom-webhook"))
        .compile
        .drain
      requests <- captured.get
    } yield {
      assert(requests.nonEmpty)
      assert(requests.forall(_.method == Method.POST))
      assert(requests.forall(_.uri == uri"http://localhost/custom-webhook"))
      assert(requests.forall(!_.hasIdempotencyKey))
      assert(requests.forall(_.body.hcursor.get[String]("type").toOption.contains("message")))
    }
  }

  test("12.interrupted source finalizer posts a synthetic cancellation stop") {
    for {
      received <- Ref.of[IO, List[Json]](Nil)
      observer = TeamsObserver[IO](Resource.pure(mockClient(received)))
      events <- service
        .eventStream(_ => IO.never)
        .take(1)
        .through(observer.observe(uri"http://localhost/webhook"))
        .compile
        .toList
      posted <- received.get
    } yield {
      assertEquals(events.size, 1)
      assert(events.head.isInstanceOf[ServiceStart])
      assertEquals(posted.size, 2)
      assert(posted.last.noSpaces.contains("ByCancellation"), posted.last.spaces2)
    }
  }

  test("13.withMaxStackTraceFrames truncates panic cards without mutating the original observer") {
    val run = for {
      limited_cards <- Ref.of[IO, List[Json]](Nil)
      original_cards <- Ref.of[IO, List[Json]](Nil)
      client_limited = Resource.pure[IO, Client[IO]](mockClient(limited_cards))
      client_original = Resource.pure[IO, Client[IO]](mockClient(original_cards))
      base_observer = TeamsObserver[IO](client_original)
      limited_observer = TeamsObserver[IO](client_limited).withMaxStackTraceFrames(0)
      panic_service = service.eventStream(_ => IO.raiseError(new RuntimeException("limited-stack")))
      _ <- panic_service.through(limited_observer.observe(uri"http://localhost/webhook")).compile.drain
      _ <- service.eventStream(_ => IO.raiseError(new RuntimeException("original-stack")))
        .through(base_observer.observe(uri"http://localhost/webhook"))
        .compile
        .drain
      limited <- limited_cards.get
      original <- original_cards.get
    } yield (limited, original)

    run.map { case (limited, original) =>
      val limited_panic = limited.find(_.noSpaces.contains("Service Panic")).get
      val original_panic = original.find(_.noSpaces.contains("Service Panic")).get
      assert(limited_panic.noSpaces.contains("\"fontType\":\"Monospace\""))
      assert(limited_panic.noSpaces.contains("\"text\":\"\""), limited_panic.spaces2)
      assert(original_panic.noSpaces.contains("original-stack"), original_panic.spaces2)
    }
  }

  test("14.Debug/log-link and throwable ReportedEvents render their missing branches") {
    val locator = new LogLocator {
      override def locate(timestamp: Instant): LogLink =
        LogLink(s"https://logs.example/${timestamp.toEpochMilli}")
    }
    val linked_service = service.updateConfig(
      _.withReportedEventThreshold(_.Debug)
        .withLogLocator(IO.pure(Some(locator)))
        .withHomepage("https://service.example"))

    linked_service
      .eventStream { agent =>
        agent.logger.debug("debug-card") >>
          agent.logger.error("error-with-stack", new RuntimeException("reported-stack"))
      }
      .compile
      .toList
      .flatMap { events =>
        val debug = events.collectFirst {
          case event: ReportedEvent if event.logRecord.message.value.noSpaces.contains("debug-card") => event
        }.get
        val error = events.collectFirst {
          case event: ReportedEvent if event.logRecord.message.value.noSpaces.contains("error-with-stack") =>
            event
        }.get
        for {
          debug_card <- TeamsTranslator[IO].translate(debug).map(_.get.asJson)
          error_card <- TeamsTranslator[IO].translate(error).map(_.get.asJson)
        } yield {
          assert(debug_card.noSpaces.contains("Dark"), debug_card.spaces2)
          assert(debug_card.noSpaces.contains("https://logs.example"), debug_card.spaces2)
          assert(debug_card.noSpaces.contains("https://service.example"), debug_card.spaces2)
          assert(error_card.noSpaces.contains("reported-stack"), error_card.spaces2)
          assert(error_card.noSpaces.contains("Monospace"), error_card.spaces2)
        }
      }
  }
}
