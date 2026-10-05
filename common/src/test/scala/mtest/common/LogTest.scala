package mtest.common

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.logging.{Log, LogLevel}
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
}
