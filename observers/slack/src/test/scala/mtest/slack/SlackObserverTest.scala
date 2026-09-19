package mtest.slack

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.{MetricsSnapshot, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.observers.slack.SlackObserver
import munit.CatsEffectSuite
import org.http4s.client.Client
import org.http4s.{Request, Response, Status, Uri}
import org.typelevel.ci.CIString

import scala.concurrent.duration.*

class SlackObserverTest extends CatsEffectSuite {

  private val webhook: Uri = Uri.unsafeFromString("https://hooks.slack.com/services/T/B/xyz")

  // A short-lived service that starts, does a tiny bit of work, and stops normally.
  private val service: fs2.Stream[IO, Event] =
    TaskGuard[IO]("slack")
      .service("observer-test")
      .eventStream(_.logger.info("hello"))

  // One captured POST: its idempotency key and JSON body.
  final private case class Captured(idempotencyKey: Option[String], body: String)

  // A recording http4s client that always returns 200 and records each request's idempotency key and body.
  private val idempotencyHeader: CIString = CIString("Idempotency-Key")

  private def recording(sent: Ref[IO, List[Captured]]): Resource[IO, Client[IO]] =
    Resource.pure(Client[IO] { (req: Request[IO]) =>
      Resource.eval(req.bodyText.compile.string.flatMap { body =>
        val key = req.headers.get(idempotencyHeader).map(_.head.value)
        sent.update(Captured(key, body) :: _)
      } *> IO.pure(Response[IO](Status.Ok)))
    })

  test("1.publishes one POST per translated event, each carrying an idempotency key") {
    Ref.of[IO, List[Captured]](Nil).flatMap { sent =>
      val slack = SlackObserver[IO](recording(sent))
      service.through(slack.observe(webhook)).compile.drain *> sent.get
    }.map { captured =>
      assert(captured.nonEmpty)
      // every publish carries an Idempotency-Key header
      assert(captured.forall(_.idempotencyKey.nonEmpty))
      // the Block Kit payload is JSON with a username and attachments
      assert(captured.forall(c => c.body.contains("username") && c.body.contains("attachments")))
    }
  }

  test("2.idempotency keys are distinct across the different events in a run") {
    Ref.of[IO, List[Captured]](Nil).flatMap { sent =>
      val slack = SlackObserver[IO](recording(sent))
      service.through(slack.observe(webhook)).compile.drain *> sent.get
    }.map(_.flatMap(_.idempotencyKey)).map { keys =>
      assert(keys.nonEmpty)
      // no two events collapse onto the same key (which would make Slack dedupe and drop one)
      assert(keys.distinct.size == keys.size)
    }
  }

  test("3.skipAll translator publishes nothing") {
    Ref.of[IO, List[Captured]](Nil).flatMap { sent =>
      val slack = SlackObserver[IO](recording(sent)).withTranslator(_.skipAll)
      service.through(slack.observe(webhook)).compile.drain *> sent.get
    }.map { captured =>
      assert(captured.isEmpty)
    }
  }

  test("4.a failing webhook does not drop events: the stream still completes with every event") {
    // publish uses `successful(...).attempt.void`, so a non-2xx response is swallowed rather than aborting
    val failing: Resource[IO, Client[IO]] =
      Resource.pure(Client[IO](_ => Resource.pure(Response[IO](Status.InternalServerError))))

    service.through(SlackObserver[IO](failing).observe(webhook)).compile.toList.map { events =>
      assert(events.exists(_.isInstanceOf[ServiceStart]))
      assert(events.exists(_.isInstanceOf[ServiceStop]))
    }
  }

  test("5.withTranslator can skip a single event type, publishing fewer than the events seen") {
    // a service that additionally emits a metrics snapshot, so skipMetricsSnapshot has something to drop
    val reporting: fs2.Stream[IO, Event] =
      TaskGuard[IO]("slack")
        .service("observer-test")
        .updateConfig(_.withRestartPolicy(1.hour, _.fixedDelay(100.millis).repeat.limited(1)))
        .eventStream(_.adhoc.report)

    Ref.of[IO, List[Captured]](Nil).flatMap { sent =>
      val slack = SlackObserver[IO](recording(sent)).withTranslator(_.skipMetricsSnapshot)
      reporting.through(slack.observe(webhook)).compile.toList.flatMap(evts => sent.get.map(_ -> evts))
    }.map { case (captured, events) =>
      assert(events.exists(_.isInstanceOf[MetricsSnapshot]))
      // the metrics snapshot was translated to nothing, so fewer POSTs than events
      assert(captured.size < events.size)
    }
  }
}
