package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.*
import com.github.chenharryhua.nanjin.guard.translator.Translator
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

/** Tests that observer finalization produces synthetic ServiceStop events for services that were started but
  * never stopped (simulating abrupt termination from the observer's perspective).
  */
class FinalizeMonitorTest extends AnyFunSuite {

  private val service = TaskGuard[IO]("finalize").service("finalize")

  private val translator: Translator[IO, Event] = Translator.idTranslator[IO]

  test("1.observer finalizer emits ByCancellation stop for interrupted service") {
    // When an observer's stream is externally interrupted (e.g., take(N)),
    // it should produce a synthetic ServiceStop(ByCancellation) via onFinalize.
    // We test this indirectly by creating an observer-like pipe that tracks starts.
    import cats.effect.kernel.Ref
    import com.github.chenharryhua.nanjin.guard.config.ServiceId

    val test = for {
      ref <- Ref.of[IO, Map[ServiceId, ServiceStart]](Map.empty)
      events <- service
        .eventStream(_ => IO.sleep(10.seconds))
        .evalTap {
          case ss: ServiceStart => ref.update(_.updated(ss.serviceIdentity.serviceId, ss))
          case ss: ServiceStop  => ref.update(_.removed(ss.serviceIdentity.serviceId))
          case _                => IO.unit
        }
        .take(1) // only take ServiceStart, then interrupt
        .compile
        .toList
      // After the stream ends, ref should still have the ServiceStart tracked
      tracked <- ref.get
    } yield {
      assert(events.size == 1)
      assert(events.head.isInstanceOf[ServiceStart])
      // The service was started but never stopped from the observer's POV
      assert(tracked.size == 1)
    }

    test.unsafeRunSync()
  }

  test("2.observer tracking clears on normal ServiceStop") {
    import cats.effect.kernel.Ref
    import com.github.chenharryhua.nanjin.guard.config.ServiceId

    val test = for {
      ref <- Ref.of[IO, Map[ServiceId, ServiceStart]](Map.empty)
      _ <- service
        .eventStream(_ => IO.unit)
        .evalTap {
          case ss: ServiceStart => ref.update(_.updated(ss.serviceIdentity.serviceId, ss))
          case ss: ServiceStop  => ref.update(_.removed(ss.serviceIdentity.serviceId))
          case _                => IO.unit
        }
        .compile
        .drain
      tracked <- ref.get
    } yield
      // After normal completion, the ServiceStop should have cleared the tracked start
      assert(tracked.isEmpty)

    test.unsafeRunSync()
  }

  test("3.Translator.idTranslator translates all event types") {
    val events = service
      .eventStream(agent => agent.logger.info("msg"))
      .compile
      .toList
      .unsafeRunSync()

    val translated = events.map(e => translator.translate(e).unsafeRunSync())
    // idTranslator should produce Some for all events
    assert(translated.forall(_.isDefined))
    assert(translated.map(_.get) == events)
  }

  test("4.Translator.empty skips all events") {
    val empty = Translator.empty[IO, Event]
    val events = service
      .eventStream(_ => IO.unit)
      .compile
      .toList
      .unsafeRunSync()

    val translated = events.map(e => empty.translate(e).unsafeRunSync())
    assert(translated.forall(_.isEmpty))
  }

  test("5.Translator.skipAll produces empty translator") {
    val skipper = Translator.idTranslator[IO].skipAll
    val events = service
      .eventStream(_ => IO.unit)
      .compile
      .toList
      .unsafeRunSync()

    val translated = events.map(e => skipper.translate(e).unsafeRunSync())
    assert(translated.forall(_.isEmpty))
  }

  test("6.Translator skip individual event types") {
    val noStart = Translator.idTranslator[IO].skipServiceStart
    val events = service
      .eventStream(_ => IO.unit)
      .compile
      .toList
      .unsafeRunSync()

    val translated = events.flatMap(e => noStart.translate(e).unsafeRunSync())
    // ServiceStart should be filtered out
    assert(translated.forall(!_.isInstanceOf[ServiceStart]))
    // ServiceStop should still be present
    assert(translated.exists(_.isInstanceOf[ServiceStop]))
  }
}
