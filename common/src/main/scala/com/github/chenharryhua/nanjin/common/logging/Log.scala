package com.github.chenharryhua.nanjin.common.logging

import cats.derived.derived
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.{Functor, MonadThrow}
import io.circe.Encoder

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
  */
final case class LogEntry[S](message: S, level: LogLevel, cause: Option[Throwable]) derives Functor

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
  protected def create[S: Encoder](message: S, level: LogLevel, cause: Option[Throwable]): F[M]
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
    */
  final def emit[S: Encoder](
    message: => S,
    level: LogLevel,
    cause: Option[Throwable]
  ): F[Unit] = {
    def process: F[Unit] = create[S](message, level, cause).flatMap(publish).attempt.void
    enabled(level).ifM(process, ().pure[F])
  }

  /** Emit a pre-built `LogEntry`, using its `level` and `cause`. Convenient when the level travels with the
    * payload rather than being chosen at the call site.
    */
  final def emit[S: Encoder](logEntry: LogEntry[S]): F[Unit] =
    emit(logEntry.message, logEntry.level, logEntry.cause)

  final def error[S: Encoder](msg: => S): F[Unit] = emit[S](msg, LogLevel.Error, None)
  final def error[S: Encoder](msg: => S, ex: Throwable): F[Unit] =
    emit[S](msg, LogLevel.Error, Some(ex))

  final def warn[S: Encoder](msg: => S): F[Unit] = emit[S](msg, LogLevel.Warn, None)
  final def warn[S: Encoder](msg: => S, ex: Throwable): F[Unit] =
    emit[S](msg, LogLevel.Warn, Some(ex))

  final def good[S: Encoder](msg: => S): F[Unit] = emit[S](msg, LogLevel.Good, None)
  final def info[S: Encoder](msg: => S): F[Unit] = emit[S](msg, LogLevel.Info, None)

  final def debug[S: Encoder](msg: => S): F[Unit] = emit[S](msg, LogLevel.Debug, None)
  final def debug[S: Encoder](msg: F[S]): F[Unit] =
    msg.attempt.flatMap {
      case Left(ex)     => emit[String]("Debug Error", LogLevel.Debug, Some(ex))
      case Right(value) => emit[S](value, LogLevel.Debug, None)
    }
end Log

object Log:
  def noop[F[_]: MonadThrow]: Log[F] = new Log[F] {
    private val unit: F[Unit] = ().pure[F]
    private val disabled: F[Boolean] = false.pure[F]

    override protected type M = Unit
    override protected def create[S: Encoder](message: S, level: LogLevel, cause: Option[Throwable]): F[M] =
      unit
    override protected def publish(event: M): F[Unit] = unit
    override protected def enabled(level: LogLevel): F[Boolean] = disabled
  }
end Log
