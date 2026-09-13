package com.github.chenharryhua.nanjin.guard.observers.splunk

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import cats.effect.unsafe.implicits.global
import cats.syntax.foldable.toFoldableOps
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event.*
import io.circe.Json
import io.circe.jawn.parse
import org.http4s.*
import org.http4s.client.Client
import org.http4s.dsl.io.*
import org.http4s.headers.Authorization
import org.http4s.implicits.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class SplunkObserverTest extends AnyFunSuite {

  private val service = TaskGuard[IO]("splunk-test")
    .service("splunk-observer-test")
    .updateConfig(_.withRestartPolicy(1.hour, _.fixedDelay(100.millis).repeat.limited(1)))

  private val endpoint: Uri = uri"http://localhost/services/collector/event"

  /** Captures each posted HEC envelope (parsed) plus the Authorization header seen on the request. */
  private def mockClient(envelopes: Ref[IO, List[Json]], auths: Ref[IO, List[String]]): Client[IO] =
    Client.fromHttpApp(HttpApp[IO] { req =>
      val recordAuth =
        req.headers.get[Authorization].traverse_(a => auths.update(_ :+ a.credentials.renderString))
      req.as[String].flatMap { body =>
        parse(body) match {
          case Right(json) => recordAuth *> envelopes.update(_ :+ json) *> Ok("1")
          case Left(_)     => BadRequest("invalid json")
        }
      }
    })

  private def hec(extra: HecConfig => HecConfig = identity): HecConfig =
    extra(HecConfig(endpoint = endpoint, token = "hec-token-123"))

  test("1.posts one HEC envelope per event, each wrapping the translated event under \"event\"") {
    val envelopes = Ref.unsafe[IO, List[Json]](Nil)
    val auths = Ref.unsafe[IO, List[String]](Nil)
    val client = Resource.pure[IO, Client[IO]](mockClient(envelopes, auths))
    val observer = SplunkObserver[IO](client)

    val events = service
      .eventStream(_ => IO.unit)
      .through(observer.observe(hec()))
      .compile
      .toList
      .unsafeRunSync()

    val posted = envelopes.get.unsafeRunSync()
    assert(events.exists(_.isInstanceOf[ServiceStart]))
    assert(events.exists(_.isInstanceOf[ServiceStop]))
    assert(posted.size == events.size)
    // every envelope carries the HEC fields
    assert(posted.forall(_.hcursor.downField("event").focus.nonEmpty))
    assert(posted.forall(_.hcursor.get[String]("sourcetype").toOption.contains("_json")))
    assert(posted.forall(_.hcursor.get[Double]("time").toOption.nonEmpty))
  }

  test("2.sends the HEC token as a Bearer Authorization header") {
    val envelopes = Ref.unsafe[IO, List[Json]](Nil)
    val auths = Ref.unsafe[IO, List[String]](Nil)
    val client = Resource.pure[IO, Client[IO]](mockClient(envelopes, auths))
    val observer = SplunkObserver[IO](client)

    service
      .eventStream(_ => IO.unit)
      .through(observer.observe(hec()))
      .compile
      .drain
      .unsafeRunSync()

    val seen = auths.get.unsafeRunSync()
    assert(seen.nonEmpty)
    assert(seen.forall(_ == "Bearer hec-token-123"))
  }

  test("3.optional index and source are included only when configured") {
    val withOpt = Ref.unsafe[IO, List[Json]](Nil)
    val withoutOpt = Ref.unsafe[IO, List[Json]](Nil)
    val auths = Ref.unsafe[IO, List[String]](Nil)

    def run(sink: Ref[IO, List[Json]], config: HecConfig): Unit = {
      val client = Resource.pure[IO, Client[IO]](mockClient(sink, auths))
      service
        .eventStream(_ => IO.unit)
        .through(SplunkObserver[IO](client).observe(config))
        .compile
        .drain
        .unsafeRunSync()
    }

    run(withOpt, hec(_.copy(index = Some("main"), source = Some("nanjin"))))
    run(withoutOpt, hec())

    val withOptJson = withOpt.get.unsafeRunSync()
    val withoutOptJson = withoutOpt.get.unsafeRunSync()
    assert(withOptJson.nonEmpty && withoutOptJson.nonEmpty)
    // present when configured
    assert(withOptJson.forall(_.hcursor.get[String]("index").toOption.contains("main")))
    assert(withOptJson.forall(_.hcursor.get[String]("source").toOption.contains("nanjin")))
    // absent when not configured
    assert(withoutOptJson.forall(_.hcursor.downField("index").focus.isEmpty))
    assert(withoutOptJson.forall(_.hcursor.downField("source").focus.isEmpty))
  }

  test("4.custom sourceType overrides the _json default") {
    val envelopes = Ref.unsafe[IO, List[Json]](Nil)
    val auths = Ref.unsafe[IO, List[String]](Nil)
    val client = Resource.pure[IO, Client[IO]](mockClient(envelopes, auths))

    service
      .eventStream(_ => IO.unit)
      .through(SplunkObserver[IO](client).observe(hec(_.copy(sourceType = Some("nanjin:event")))))
      .compile
      .drain
      .unsafeRunSync()

    val posted = envelopes.get.unsafeRunSync()
    assert(posted.nonEmpty)
    assert(posted.forall(_.hcursor.get[String]("sourcetype").toOption.contains("nanjin:event")))
  }

  test("5.survives HEC failure without dropping events") {
    val failClient = Client.fromHttpApp(HttpApp[IO](_ => InternalServerError("boom")))
    val client = Resource.pure[IO, Client[IO]](failClient)
    val observer = SplunkObserver[IO](client)

    val events = service
      .eventStream(_ => IO.unit)
      .through(observer.observe(hec()))
      .compile
      .toList
      .unsafeRunSync()

    assert(events.exists(_.isInstanceOf[ServiceStart]))
    assert(events.exists(_.isInstanceOf[ServiceStop]))
  }

  test("6.withTranslator allows skipping event types") {
    val envelopes = Ref.unsafe[IO, List[Json]](Nil)
    val auths = Ref.unsafe[IO, List[String]](Nil)
    val client = Resource.pure[IO, Client[IO]](mockClient(envelopes, auths))
    val observer = SplunkObserver[IO](client).withTranslator(_.skipMetricsSnapshot)

    val events = service
      .eventStream(agent => agent.adhoc.report)
      .through(observer.observe(hec()))
      .compile
      .toList
      .unsafeRunSync()

    val posted = envelopes.get.unsafeRunSync()
    assert(events.exists(_.isInstanceOf[MetricsSnapshot]))
    assert(posted.size < events.size)
  }
}
