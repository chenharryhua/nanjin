package com.github.chenharryhua.nanjin.guard.observers.teams

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event.*
import io.circe.Json
import io.circe.jawn.parse
import org.http4s.*
import org.http4s.client.Client
import org.http4s.dsl.io.*
import org.http4s.implicits.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class TeamsObserverTest extends AnyFunSuite {

  private val service = TaskGuard[IO]("teams-test")
    .service("teams-observer-test")
    .updateConfig(_.withRestartPolicy(1.hour, _.fixedDelay(100.millis).repeat.limited(1)))

  private def mockClient(received: Ref[IO, List[Json]]): Client[IO] =
    Client.fromHttpApp(HttpApp[IO] { req =>
      req.as[String].flatMap { body =>
        parse(body) match {
          case Right(json) => received.update(_ :+ json) *> Ok("1")
          case Left(_)     => BadRequest("invalid json")
        }
      }
    })

  // --- Observer integration tests ---

  test("1.TeamsObserver posts cards to webhook for each event") {
    val received = Ref.unsafe[IO, List[Json]](Nil)
    val client = Resource.pure[IO, Client[IO]](mockClient(received))
    val observer = TeamsObserver[IO](client)

    val events = service
      .eventStream(_ => IO.unit)
      .through(observer.observe(uri"http://localhost/webhook"))
      .compile
      .toList
      .unsafeRunSync()

    val posted = received.get.unsafeRunSync()
    assert(events.exists(_.isInstanceOf[ServiceStart]))
    assert(events.exists(_.isInstanceOf[ServiceStop]))
    assert(posted.size == events.size)
  }

  test("2.TeamsObserver survives webhook failure without dropping events") {
    val failClient = Client.fromHttpApp(HttpApp[IO](_ => InternalServerError("boom")))
    val client = Resource.pure[IO, Client[IO]](failClient)
    val observer = TeamsObserver[IO](client)

    val events = service
      .eventStream(_ => IO.unit)
      .through(observer.observe(uri"http://localhost/webhook"))
      .compile
      .toList
      .unsafeRunSync()

    assert(events.exists(_.isInstanceOf[ServiceStart]))
    assert(events.exists(_.isInstanceOf[ServiceStop]))
  }

  test("3.withTranslator allows skipping event types") {
    val received = Ref.unsafe[IO, List[Json]](Nil)
    val client = Resource.pure[IO, Client[IO]](mockClient(received))
    val observer = TeamsObserver[IO](client).withTranslator(_.skipMetricsSnapshot)

    val events = service
      .eventStream(agent => agent.adhoc.report)
      .through(observer.observe(uri"http://localhost/webhook"))
      .compile
      .toList
      .unsafeRunSync()

    val posted = received.get.unsafeRunSync()
    assert(events.exists(_.isInstanceOf[MetricsSnapshot]))
    assert(posted.size < events.size)
  }

  // --- Per-event card content tests ---

  private lazy val allEvents: List[com.github.chenharryhua.nanjin.guard.event.Event] = service
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

  private def translateEvent(
    pf: PartialFunction[com.github.chenharryhua.nanjin.guard.event.Event, Boolean]): Json = {
    val translator = TeamsTranslator[IO]
    val evt = allEvents.find(pf.isDefinedAt).get
    val card = translator.translate(evt).unsafeRunSync().get
    summon[io.circe.Encoder[AdaptiveCard]].apply(card)
  }

  test("4.ServiceStart card contains service name and index") {
    val json = translateEvent { case _: ServiceStart => true }
    val text = json.noSpaces
    assert(text.contains("teams-observer-test"))
    assert(text.contains("Index"))
    assert(text.contains("Start Service"))
  }

  test("5.ServicePanic card contains stack trace and exception message") {
    val json = translateEvent { case _: ServicePanic => true }
    val text = json.noSpaces
    assert(text.contains("panic-test"))
    assert(text.contains("RuntimeException"))
    assert(text.contains("Service Panic"))
  }

  test("6.ServiceStop card contains stop reason") {
    val json = translateEvent { case _: ServiceStop => true }
    val text = json.noSpaces
    assert(text.contains("Stop Service"))
    assert(text.contains("teams-observer-test"))
  }

  test("7.MetricsSnapshot card contains snapshot data") {
    val json = translateEvent { case _: MetricsSnapshot => true }
    val text = json.noSpaces
    assert(text.contains("Metrics Report"))
    assert(text.contains("teams-observer-test"))
  }

  test("8.ReportedEvent Info card contains correlation and message") {
    val json = translateEvent {
      case e: ReportedEvent if e.message.value.noSpaces.contains("info-msg") => true
    }
    val text = json.noSpaces
    assert(text.contains("info-msg"))
    assert(text.contains("Correlation"))
    assert(text.contains("Info"))
  }

  test("9.ReportedEvent Warn card has warning color") {
    val json = translateEvent {
      case e: ReportedEvent if e.message.value.noSpaces.contains("warn-msg") => true
    }
    val text = json.noSpaces
    assert(text.contains("warn-msg"))
    assert(text.contains("Warning"))
  }

  test("10.ReportedEvent Error card has error color") {
    val json = translateEvent {
      case e: ReportedEvent if e.message.value.noSpaces.contains("error-msg") => true
    }
    val text = json.noSpaces
    assert(text.contains("error-msg"))
    assert(text.contains("Attention"))
  }
}
