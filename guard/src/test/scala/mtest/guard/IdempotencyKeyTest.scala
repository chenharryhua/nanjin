package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.{ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.observers.idempotencyKey
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class IdempotencyKeyTest extends AnyFunSuite {

  // A short-lived service that starts, reports metrics once, logs, and stops normally, so the stream carries
  // a representative mix of event types.
  private val events: List[Event] =
    TaskGuard[IO]("idem")
      .service("idem-key")
      .updateConfig(_.withReportPolicy(_.crontab(_.secondly).repeat))
      .eventStream(_.logger.info("hello"))
      .interruptAfter(3.seconds)
      .compile
      .toList
      .unsafeRunSync()

  test("1.every event yields a non-empty key") {
    assert(events.nonEmpty)
    assert(events.forall(e => idempotencyKey(e).nonEmpty))
  }

  test("2.the key is stable: the same event always maps to the same key") {
    assert(events.forall(e => idempotencyKey(e) == idempotencyKey(e)))
  }

  test("3.the key embeds the serviceId so different services never collide") {
    assert(events.forall(e => idempotencyKey(e).startsWith(e.serviceIdentity.serviceId.value.toString)))
  }

  test("4.ServiceStart keys carry the start tag and tick index") {
    val starts = events.collect { case e: ServiceStart => e }
    assert(starts.nonEmpty)
    starts.foreach(e =>
      assert(idempotencyKey(e) == s"${e.serviceIdentity.serviceId.value}-start-${e.tick.index}"))
  }

  test("5.ServiceStop keys carry the stop tag") {
    val stops = events.collect { case e: ServiceStop => e }
    assert(stops.nonEmpty)
    stops.foreach(e => assert(idempotencyKey(e) == s"${e.serviceIdentity.serviceId.value}-stop"))
  }

  test("6.keys are distinct across events of different kinds within one run") {
    // start, stop, and each periodic metrics snapshot get their own key; no two collapse.
    val keys = events.map(idempotencyKey)
    assert(keys.distinct.size == keys.size)
  }
}
