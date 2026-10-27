package mtest.guard

import cats.effect.IO
import cats.syntax.all.*
import com.comcast.ip4s.port
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event.{ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.{Event, StopReason}
import munit.CatsEffectSuite
import org.http4s.Method.POST
import org.http4s.Request
import org.http4s.Uri
import org.http4s.ember.client.EmberClientBuilder

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** Verification tests for the racing-terminal-stop findings M1/M2/M3 in the service-lifecycle review.
  *
  * The review confirmed (no High bug) that `channel.closeWithElement` is first-close-wins and the
  * `serviceCancel` `isClosed` guard suppresses a second stop, so there is never a double-stop, a lost
  * `ServiceStop`, or a hang — but the "exactly one `ServiceStop`, and no trailing `ByCancellation` after a
  * natural terminal stop" safety rested incidentally on timing and was not pinned by a test that races an
  * external `POST /stop` against the restart loop. These tests pin that safety. They do NOT assert a
  * deterministic `StopReason` under a race (the review documents that as intentionally nondeterministic);
  * they assert the invariants that must hold regardless of scheduling. Production source is not modified.
  */
class ServiceStopRaceTest extends CatsEffectSuite {

  private val guard = TaskGuard[IO]("service.stop.race")

  /** Fire `POST /stop` after `after`, retrying on connection-refused until the ember server is bound (it
    * starts asynchronously with the service), so the stop reliably reaches an active restart loop rather than
    * racing server startup. Gives up after a bounded number of attempts.
    */
  private def postStop(p: Int, after: FiniteDuration): IO[Unit] =
    EmberClientBuilder
      .default[IO]
      .build
      .use { c =>
        val uri = Uri.unsafeFromString(s"http://localhost:$p/stop")
        val attempt = c.expect[String](Request[IO](method = POST, uri = uri))
        def retrying(remaining: Int): IO[Unit] =
          attempt.void.handleErrorWith { e =>
            if (remaining > 0) IO.sleep(100.millis) >> retrying(remaining - 1)
            else IO.raiseError(e)
          }
        retrying(30)
      }
      .delayBy(after)

  // NV1: POST /stop (Maintenance) while the service is actively restarting (mid-snooze).
  test("1.stop during an active restart loop yields exactly one ServiceStop and no trailing cancellation") {
    val run = guard
      .service("stop-during-restart")
      .updateConfig(
        _.withRestartPolicy(1.hour, _.fixedDelay(500.millis).repeat) // long-ish snooze, never exhausts
          .withHttpServer(_.withPort(port"9980")))
      // fails immediately, so the loop spends almost all its time asleep between retries
      .eventStream(_ => IO.raiseError(new Exception("fail")))
      .map(checkJson)
      .compile
      .toList

    // fire the stop ~1.2s in, which lands during one of the 500ms snoozes
    (run, postStop(9980, 1200.millis)).parMapN { (events, _) =>
      val stops = events.collect { case s: ServiceStop => s }
      // exactly one terminal stop
      assertEquals(stops.size, 1)
      // it is the last event, and it is not a spurious trailing ByCancellation appended after the fact
      assert(events.last.isInstanceOf[ServiceStop])
      assertEquals(stops.head.cause, StopReason.Maintenance)
      // the loop did start at least once (it was genuinely in the restart loop)
      assert(events.count(_.isInstanceOf[ServiceStart]) >= 1)
    }
  }

  // NV2: a near-exhaustion policy failing immediately, racing an external POST /stop, repeated.
  test(
    "2.stop racing policy exhaustion always yields exactly one ServiceStop, cause Maintenance or ByException") {
    def once(stopDelay: FiniteDuration): IO[List[Event]] = {
      val run = guard
        .service("stop-vs-exhaustion")
        .updateConfig(
          _.withRestartPolicy(1.hour, _.fixedDelay(150.millis).repeat.limited(3)) // exhausts after 3 retries
            .withHttpServer(_.withPort(port"9981")))
        .eventStream(_ => IO.raiseError(new Exception("fail")))
        .map(checkJson)
        .compile
        .toList
      // race the stop against the natural exhaustion (which happens around 3 * 150ms = 450ms)
      (run, postStop(9981, stopDelay).attempt).parMapN((events, _) => events)
    }

    // vary the stop timing across the exhaustion window to exercise both orderings
    val delays = List(300.millis, 400.millis, 450.millis, 500.millis, 600.millis)
    delays.traverse(once).map { runs =>
      runs.foreach { events =>
        val stops = events.collect { case s: ServiceStop => s }
        assertEquals(stops.size, 1, s"expected exactly one ServiceStop, got ${stops.map(_.cause)}")
        assert(events.last.isInstanceOf[ServiceStop])
        val cause = stops.head.cause
        assert(
          cause == StopReason.Maintenance || cause.isInstanceOf[StopReason.ByException],
          s"unexpected terminal cause under race: $cause")
      }
    }
  }

  // NV3: many natural completions/exhaustions must never append a ByCancellation after a natural stop.
  test("3.natural termination never appends a trailing ByCancellation") {
    def completes: IO[List[Event]] =
      guard.service("natural-complete").eventStream(_ => IO.unit).map(checkJson).compile.toList

    def exhausts: IO[List[Event]] =
      guard
        .service("natural-exhaust")
        .updateConfig(_.withRestartPolicy(1.hour, _.fixedDelay(50.millis).repeat.limited(2)))
        .eventStream(_ => IO.raiseError(new Exception("fail")))
        .map(checkJson)
        .compile
        .toList

    val runs: List[IO[List[Event]]] = List.fill(10)(completes) ::: List.fill(10)(exhausts)

    runs.sequence.map { allRuns =>
      allRuns.foreach { events =>
        val stops = events.collect { case s: ServiceStop => s }
        assertEquals(stops.size, 1)
        val cause = stops.head.cause
        // a natural stop is Successfully or ByException; never ByCancellation, and never followed by one
        assert(
          cause == StopReason.Successfully || cause.isInstanceOf[StopReason.ByException],
          s"unexpected natural cause: $cause")
        assert(events.last.isInstanceOf[ServiceStop])
      }
    }
  }
}
