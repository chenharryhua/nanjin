package mtest.common

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.logging.{Log, LogLevel}
import io.circe.Encoder
import io.circe.syntax.given
import org.scalatest.funsuite.AnyFunSuite

class LogTest extends AnyFunSuite {

  final private class RecordingLog(enabledLevels: Set[LogLevel]) extends Log[IO] {
    protected type M = (LogLevel, String, Option[Throwable])

    private var events: Vector[M] = Vector.empty

    protected def create[A: Encoder](message: A, level: LogLevel, stackTrace: Option[Throwable]): IO[M] =
      IO.pure((level, message.asJson.noSpaces, stackTrace))

    protected def publish(event: M): IO[Unit] =
      IO { events = events :+ event }

    protected def enabled(level: LogLevel): IO[Boolean] =
      IO.pure(enabledLevels.contains(level))

    def snapshot: Vector[M] = events
  }

  test("logs enabled messages with their level and optional exception") {
    val log = new RecordingLog(Set(LogLevel.Error, LogLevel.Warn))
    val ex = new IllegalStateException("boom")

    log.error("failed", ex).unsafeRunSync()

    assert(log.snapshot == Vector((LogLevel.Error, "\"failed\"", Some(ex))))
  }

  test("does not evaluate a message when the level is disabled") {
    val log = new RecordingLog(Set.empty)

    val boom = new RuntimeException("should not be evaluated")
    def msg: String = throw boom

    try {
      log.warn(msg).unsafeRunSync()
      assert(log.snapshot.isEmpty)
    } catch {
      case ex: Throwable => fail(s"expected no exception, got ${ex.getMessage}")
    }
  }

  test("debug effect logs the value on success and a fallback message on failure") {
    val log = new RecordingLog(Set(LogLevel.Debug))

    log.debug(IO.pure("ok")).unsafeRunSync()
    assert(log.snapshot == Vector((LogLevel.Debug, "\"ok\"", None)))

    val failure = new RuntimeException("broken")
    log.debug(IO.raiseError[String](failure)).unsafeRunSync()
    assert(log.snapshot.last == (LogLevel.Debug, "\"Debug Error\"", Some(failure)))
  }

  test("noop logger ignores messages without throwing") {
    val log = Log.noop[IO]

    try {
      log.good("silent").unsafeRunSync()
      log.debug(IO.raiseError[String](new RuntimeException("should be ignored"))).unsafeRunSync()
    } catch {
      case ex: Throwable => fail(s"expected no exception, got ${ex.getMessage}")
    }
  }

  test("LogLevel exposes the expected ordering and encoding") {
    assert(LogLevel.Error.value > LogLevel.Warn.value)
    assert(LogLevel.Warn.value > LogLevel.Info.value)
    assert(LogLevel.Info.value > LogLevel.Debug.value)
    assert(LogLevel.Good.value > LogLevel.Info.value)

    assert(LogLevel.Error.name === "ERROR")
    assert(LogLevel.Debug.name === "DEBUG")
  }
}
