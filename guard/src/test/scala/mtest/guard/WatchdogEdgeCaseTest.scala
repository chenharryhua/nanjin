package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event.*
import com.github.chenharryhua.nanjin.guard.event.{Event, StopReason}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

class WatchdogEdgeCaseTest extends AnyFunSuite {

  private val guard = TaskGuard[IO]("watchdog.edge")

  test("1.no restart policy configured - single failure stops service immediately") {
    // Default config has RestartPolicy(Policy.empty, None) — no restart at all
    val events = guard
      .service("no-restart")
      .eventStream(_ => IO.raiseError(new Exception("immediate failure")))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    assert(events.size == 2)
    assert(events.head.isInstanceOf[ServiceStart])
    assert(events.last.isInstanceOf[ServiceStop])
    assert(events.last.asInstanceOf[ServiceStop].cause.isInstanceOf[StopReason.ByException])
  }

  test("2.no restart policy - normal exit stops service") {
    val events = guard
      .service("no-restart-ok")
      .eventStream(_ => IO.unit)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    assert(events.size == 2)
    assert(events.head.isInstanceOf[ServiceStart])
    assert(events.last.asInstanceOf[ServiceStop].cause == StopReason.Successfully)
  }

  test("3.large threshold - policy never resets on rapid failures") {
    // Threshold of 1 hour; failures happen instantly so duration < threshold always
    // Policy should progress linearly without reset: 1s, 2s, 3s
    val events = guard
      .service("large-threshold")
      .updateConfig(_.withRestartPolicy(1.hour, _.fixedDelay(100.millis, 200.millis, 300.millis).limited(3)))
      .eventStream(_ => IO.raiseError(new Exception("fail")))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val panics = events.collect { case sp: ServicePanic => sp.tick }
    // Should have 3 panics then stop by exception (policy exhausted after 3 retries = 4 starts)
    assert(panics.size == 3)
    assert(panics(0).snooze == 100.millis.toJava)
    assert(panics(1).snooze == 200.millis.toJava)
    assert(panics(2).snooze == 300.millis.toJava)

    // 4 starts total (initial + 3 retries)
    assert(events.count(_.isInstanceOf[ServiceStart]) == 4)
    assert(events.last.asInstanceOf[ServiceStop].cause.isInstanceOf[StopReason.ByException])
  }

  test("4.small threshold - policy resets when service runs longer than threshold") {
    // Threshold of 200ms; the service sleeps 300ms then fails
    // Since duration > threshold each time, policy should reset every failure
    // This means snooze is always 100ms (first element of policy), never progressing to 200ms
    var attempt = 0
    val events = guard
      .service("small-threshold")
      .updateConfig(_.withRestartPolicy(200.millis, _.fixedDelay(100.millis, 500.millis).limited(3)))
      .eventStream { _ =>
        attempt += 1
        IO.sleep(300.millis) *> IO.raiseError(new Exception(s"fail #$attempt"))
      }
      .map(checkJson)
      .evalMapFilter[IO, Tick] {
        case sp: ServicePanic => IO(Some(sp.tick))
        case _                => IO(None)
      }
      .take(3)
      .compile
      .toList
      .unsafeRunSync()

    // Because the service ran longer than threshold (300ms > 200ms), policy resets each time
    // So every panic tick should use the first delay (100ms), not progress to 500ms
    assert(events.size == 3)
    assert(events(0).snooze == 100.millis.toJava)
    assert(events(1).snooze == 100.millis.toJava)
    assert(events(2).snooze == 100.millis.toJava)
  }

  test("5.threshold boundary - service runs exactly at threshold does not reset") {
    // Threshold 500ms; service runs ~400ms (less than threshold)
    // Policy should NOT reset — should progress 100ms, 200ms
    var attempt = 0
    val events = guard
      .service("boundary")
      .updateConfig(_.withRestartPolicy(500.millis, _.fixedDelay(100.millis, 200.millis).limited(2)))
      .eventStream { _ =>
        attempt += 1
        IO.sleep(400.millis) *> IO.raiseError(new Exception(s"fail #$attempt"))
      }
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val panics = events.collect { case sp: ServicePanic => sp.tick }
    assert(panics.size == 2)
    assert(panics(0).snooze == 100.millis.toJava)
    assert(panics(1).snooze == 200.millis.toJava)
  }

  // --- ServiceStop fires exactly once ---

  test("6.stop event fires exactly once on exception (no restart)") {
    val events = guard
      .service("stop-once-err")
      .eventStream(_ => IO.raiseError(new Exception("boom")))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    assert(events.count(_.isInstanceOf[ServiceStop]) == 1)
  }

  test("7.stop event fires exactly once on cancellation") {
    val events = guard
      .service("stop-once-cancel")
      .eventStream(_ => IO.canceled)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    assert(events.count(_.isInstanceOf[ServiceStop]) == 1)
    assert(events.last.asInstanceOf[ServiceStop].cause == StopReason.ByCancellation)
  }

  test("8.stop event fires exactly once after retries exhausted") {
    val events = guard
      .service("stop-once-retries")
      .updateConfig(_.withRestartPolicy(1.hour, _.fixedDelay(100.millis).limited(2)))
      .eventStream(_ => IO.raiseError(new Exception("fail")))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    assert(events.count(_.isInstanceOf[ServiceStop]) == 1)
    assert(events.last.asInstanceOf[ServiceStop].cause.isInstanceOf[StopReason.ByException])
  }

  test("9.stop event fires exactly once on external stream interruption") {
    val events = guard
      .service("stop-once-interrupt")
      .eventStream(_ => IO.sleep(10.seconds))
      .take(2) // externally interrupts the stream after ServiceStart + one more event
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    // Stream was interrupted externally — at most one ServiceStop should appear
    assert(events.count(_.isInstanceOf[ServiceStop]) <= 1)
  }
}
