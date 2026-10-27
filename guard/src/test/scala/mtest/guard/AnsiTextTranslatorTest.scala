package mtest.guard

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStop
import com.github.chenharryhua.nanjin.guard.event.StopReason
import com.github.chenharryhua.nanjin.guard.translator.AnsiTextTranslator
import munit.CatsEffectSuite

/** Rendering tests for `AnsiTextTranslator` (report item M1 / NV1).
  *
  * Drives a real service to produce genuine events, then renders them with the text translator. The focus is
  * the `ServiceStop(ByException)` case: the text sink is where an operator reads why a service died, and the
  * fix must surface the stack trace there (it previously rendered only the `ByException` tag, because
  * `Show[StopReason]` drops the trace). Production source other than the translator fix is unchanged.
  */
class AnsiTextTranslatorTest extends CatsEffectSuite {

  private val marker = "ansi-text-boom-marker"

  // A no-restart service whose effect throws: yields ServiceStart then ServiceStop(ByException).
  private def stopByException: IO[ServiceStop] =
    TaskGuard[IO]("ansi.text")
      .service("by-exception")
      .eventStream(_ => IO.raiseError(new Exception(marker)))
      .map(checkJson)
      .compile
      .toList
      .map(_.collectFirst { case s: ServiceStop => s }.get)

  private def render(stop: ServiceStop): IO[String] =
    AnsiTextTranslator[IO].translate(stop).map(_.getOrElse(fail("translator produced no output")))

  test("1.service_stop by exception renders the stack trace, not just the tag") {
    stopByException.flatMap(render).map { text =>
      // the terminal cause line is present
      assert(text.contains("StopReason:ByException"), s"missing cause line in:\n$text")
      // and, crucially, the exception's message/frames are surfaced (the fix); previously absent
      assert(text.contains(marker), s"stack trace not rendered in:\n$text")
    }
  }

  test("2.the stop cause really is ByException") {
    stopByException.map(stop => assert(stop.cause.isInstanceOf[StopReason.ByException]))
  }

  test("3.a normal (Successfully) stop renders its tag and no spurious trace") {
    val run = TaskGuard[IO]("ansi.text")
      .service("ok")
      .eventStream(_ => IO.unit)
      .map(checkJson)
      .compile
      .toList
      .map(_.collectFirst { case s: ServiceStop => s }.get)

    run.flatMap(render).map { text =>
      assert(text.contains("StopReason:Successfully"), s"missing cause line in:\n$text")
      // no StackTrace label is appended for a non-exception stop
      assert(!text.contains("StackTrace:"), s"unexpected trace on a Successfully stop:\n$text")
    }
  }

  test("4.the rendered stop still carries the service identity header") {
    stopByException.flatMap(render).map { text =>
      // service_event header is present (sanity that the fix didn't break the surrounding render)
      assert(text.contains("by-exception"), s"missing service name in:\n$text")
    }
  }
}
