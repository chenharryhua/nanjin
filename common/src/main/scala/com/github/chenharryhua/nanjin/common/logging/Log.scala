package com.github.chenharryhua.nanjin.common.logging

import cats.derived.derived
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.{Functor, MonadThrow, Show}
import com.github.chenharryhua.nanjin.common.OpaqueLift
import io.circe.{Decoder, Encoder}

/** Mapped Diagnostic Context carried alongside a log record. */
opaque type MDC = Map[String, String]
object MDC {
  /** Create a context from string key-value pairs. */
  def apply(map: Map[String, String]): MDC = map

  /** An empty context for log records without diagnostic metadata. */
  val empty: MDC = Map.empty[String, String]

  /** Return the context entries as a regular map. */
  extension (mdc: MDC) def value: Map[String, String] = mdc

  given Show[MDC] = _.map((k, v) => s"$k=$v").mkString(",")
  given Encoder[MDC] = OpaqueLift.lift[MDC, Map[String, String], Encoder]
  given Decoder[MDC] = OpaqueLift.lift[MDC, Map[String, String], Decoder]
}

/** A self-contained log record: the payload to encode, the level to log it at, and an optional throwable to
  * attach as a stack trace.
  *
  * Deriving `Functor` lets a caller transform the payload while keeping `level` and `cause` fixed — e.g.
  * `entry.map(_.render)` to turn a domain value into its JSON form before handing the entry to `Log.emit`.
  *
  * @param message
  *   the payload to log; encoded via its `Encoder` when emitted
  * @param level
  *   the level to log at; also gates whether the record is emitted at all (see `Log.emit`)
  * @param cause
  *   an optional throwable behind this record; its stack trace is attached when present
  * @param mdc
  *   Mapped Diagnostic Context
  */
final case class LogEntry[S](message: S, level: LogLevel, cause: Option[Throwable], mdc: MDC) derives Functor

/** Effectful, level-aware logger.
  *
  * Implementations supply the SPI (`create`/`publish`/`enabled`); the public API is expressed entirely in
  * terms of `emit`. `emit` is the single primitive — it encodes the payload, gates on `enabled(level)` so a
  * disabled level does no work, and publishes; every named helper (`error`, `warn`, `good`, `info`, `debug`)
  * simply pins a `LogLevel` and delegates to it. Use `emit` directly when the level is dynamic (for example a
  * pre-built `LogEntry`), and the named helpers otherwise.
  */
abstract class Log[F[_]: MonadThrow]:
  /*
   * Log SPI
   */
  protected type M // middleman
  protected def create[S: Encoder](message: S, level: LogLevel, cause: Option[Throwable], mdc: MDC): F[M]
  protected def publish(event: M): F[Unit]
  protected def enabled(level: LogLevel): F[Boolean]

  /*
   * Log API
   */

  /** Emit a record at the given level. Does nothing when the level is disabled, so `message` is only forced
    * and encoded when it will actually be logged. Publishing failures are swallowed — logging never disrupts
    * the surrounding effect.
    *
    * @param message
    *   the payload to log, forced only when the level is enabled
    * @param level
    *   the level to log at; the record is skipped entirely when this level is disabled
    * @param cause
    *   an optional throwable whose stack trace is attached
    * @param mdc
    *   mapped diagnostic context carried with the record
    */
  final def emit[S: Encoder](
    message: => S,
    level: LogLevel,
    cause: Option[Throwable],
    mdc: MDC
  ): F[Unit] = {
    def process: F[Unit] = create[S](message, level, cause, mdc).flatMap(publish).attempt.void
    enabled(level).ifM(process, ().pure[F])
  }

  /** Emit a pre-built `LogEntry`, using its `level` and `cause`. Convenient when the level travels with the
    * payload rather than being chosen at the call site.
    */
  final def emit[S: Encoder](logEntry: LogEntry[S]): F[Unit] =
    emit(logEntry.message, logEntry.level, logEntry.cause, logEntry.mdc)

  final def error[S: Encoder](msg: => S): F[Unit] =
    emit[S](msg, LogLevel.Error, None, MDC.empty)
  final def error[S: Encoder](msg: => S, ex: Throwable): F[Unit] =
    emit[S](msg, LogLevel.Error, Some(ex), MDC.empty)
  final def error[S: Encoder](mdc: Map[String, String], msg: => S): F[Unit] =
    emit[S](msg, LogLevel.Error, None, MDC(mdc))
  final def error[S: Encoder](mdc: Map[String, String], msg: => S, ex: Throwable): F[Unit] =
    emit[S](msg, LogLevel.Error, Some(ex), MDC(mdc))

  final def warn[S: Encoder](msg: => S): F[Unit] =
    emit[S](msg, LogLevel.Warn, None, MDC.empty)
  final def warn[S: Encoder](msg: => S, ex: Throwable): F[Unit] =
    emit[S](msg, LogLevel.Warn, Some(ex), MDC.empty)
  final def warn[S: Encoder](mdc: Map[String, String], msg: => S): F[Unit] =
    emit[S](msg, LogLevel.Warn, None, MDC(mdc))
  final def warn[S: Encoder](mdc: Map[String, String], msg: => S, ex: Throwable): F[Unit] =
    emit[S](msg, LogLevel.Warn, Some(ex), MDC(mdc))

  final def good[S: Encoder](msg: => S): F[Unit] =
    emit[S](msg, LogLevel.Good, None, MDC.empty)
  final def info[S: Encoder](msg: => S): F[Unit] =
    emit[S](msg, LogLevel.Info, None, MDC.empty)
  final def good[S: Encoder](mdc: Map[String, String], msg: => S): F[Unit] =
    emit[S](msg, LogLevel.Good, None, MDC(mdc))
  final def info[S: Encoder](mdc: Map[String, String], msg: => S): F[Unit] =
    emit[S](msg, LogLevel.Info, None, MDC(mdc))

  final def debug[S: Encoder](msg: => S): F[Unit] =
    emit[S](msg, LogLevel.Debug, None, MDC.empty)
  final def debug[S: Encoder](mdc: Map[String, String], msg: => S): F[Unit] =
    emit[S](msg, LogLevel.Debug, None, MDC(mdc))
end Log

object Log:
  def noop[F[_]: MonadThrow]: Log[F] = new Log[F] {
    private val unit: F[Unit] = ().pure[F]
    private val disabled: F[Boolean] = false.pure[F]

    override protected type M = Unit
    override protected def create[S: Encoder](
      message: S,
      level: LogLevel,
      cause: Option[Throwable],
      mdc: MDC): F[M] =
      unit
    override protected def publish(event: M): F[Unit] = unit
    override protected def enabled(level: LogLevel): F[Boolean] = disabled
  }
end Log
