package mtest.common

import cats.Functor
import cats.effect.IO
import com.github.chenharryhua.nanjin.common.logging.{Log, LogEntry, LogLevel}
import io.circe.Encoder
import io.circe.syntax.given
import munit.CatsEffectSuite

class LogTest extends CatsEffectSuite {

  final private class RecordingLog(enabledLevels: Set[LogLevel]) extends Log[IO] {
    protected type M = (LogLevel, String, Option[Throwable])

    private var events: Vector[M] = Vector.empty

    protected def create[A: Encoder](message: A, level: LogLevel, cause: Option[Throwable]): IO[M] =
      IO.pure((level, message.asJson.noSpaces, cause))

    protected def publish(event: M): IO[Unit] =
      IO { events = events :+ event }

    protected def enabled(level: LogLevel): IO[Boolean] =
      IO.pure(enabledLevels.contains(level))

    def snapshot: Vector[M] = events
  }

  test("1.logs enabled messages with their level and optional exception") {
    val log = new RecordingLog(Set(LogLevel.Error, LogLevel.Warn))
    val ex = new IllegalStateException("boom")

    log.error("failed", ex).map { _ =>
      assertEquals(log.snapshot, Vector((LogLevel.Error, "\"failed\"", Some(ex))))
    }
  }

  test("2.does not evaluate a message when the level is disabled") {
    val log = new RecordingLog(Set.empty)

    val boom = new RuntimeException("should not be evaluated")
    def msg: String = throw boom

    log.warn(msg).map { _ =>
      assert(log.snapshot.isEmpty)
    }
  }

  test("3.logs enabled debug messages") {
    val log = new RecordingLog(Set(LogLevel.Debug))

    log.debug("ok").map { _ =>
      assertEquals(log.snapshot, Vector((LogLevel.Debug, "\"ok\"", None)))
    }
  }

  test("4.does not evaluate a debug message when debug is disabled") {
    val log = new RecordingLog(Set.empty)
    val boom = new RuntimeException("should not be evaluated")
    def msg: String = throw boom

    log.debug(msg).map { _ =>
      assert(log.snapshot.isEmpty)
    }
  }

  test("5.noop logger ignores messages without throwing") {
    val log = Log.noop[IO]

    for {
      _ <- log.good("silent")
      _ <- log.debug("silent")
    } yield ()
  }

  test("6.LogLevel exposes the expected ordering and encoding") {
    assert(LogLevel.Error.value > LogLevel.Warn.value)
    assert(LogLevel.Warn.value > LogLevel.Info.value)
    assert(LogLevel.Info.value > LogLevel.Debug.value)
    assert(LogLevel.Good.value > LogLevel.Info.value)
  }

  private val allLevels: Set[LogLevel] =
    Set(LogLevel.Error, LogLevel.Warn, LogLevel.Good, LogLevel.Info, LogLevel.Debug)

  test("7.emit at an enabled level creates and publishes the record") {
    val log = new RecordingLog(allLevels)
    log.emit("payload", LogLevel.Info, None).map { _ =>
      assertEquals(log.snapshot, Vector((LogLevel.Info, "\"payload\"", None)))
    }
  }

  test("8.named helpers pin the expected level and cause") {
    val log = new RecordingLog(allLevels)
    val boom = new IllegalStateException("boom")
    for {
      _ <- log.error("e")
      _ <- log.error("e2", boom)
      _ <- log.warn("w")
      _ <- log.warn("w2", boom)
      _ <- log.good("g")
      _ <- log.info("i")
      _ <- log.debug("d")
    } yield assertEquals(
      log.snapshot,
      Vector(
        (LogLevel.Error, "\"e\"", None),
        (LogLevel.Error, "\"e2\"", Some(boom)),
        (LogLevel.Warn, "\"w\"", None),
        (LogLevel.Warn, "\"w2\"", Some(boom)),
        (LogLevel.Good, "\"g\"", None),
        (LogLevel.Info, "\"i\"", None),
        (LogLevel.Debug, "\"d\"", None)
      )
    )
  }

  test("9.emit(LogEntry) delegates using the entry level and cause") {
    val log = new RecordingLog(allLevels)
    val boom = new RuntimeException("cause")
    val entry = LogEntry("msg", LogLevel.Warn, Some(boom))
    log.emit(entry).map { _ =>
      assertEquals(log.snapshot, Vector((LogLevel.Warn, "\"msg\"", Some(boom))))
    }
  }

  test("10.LogEntry derives a Functor that maps the message and keeps level and cause") {
    val boom = new RuntimeException("cause")
    val entry = LogEntry(3, LogLevel.Good, Some(boom))
    val mapped = Functor[LogEntry].map(entry)(_ * 2)
    assertEquals(mapped.message, 6)
    assertEquals(mapped.level, LogLevel.Good)
    assertEquals(mapped.cause, Some(boom))
  }
}
