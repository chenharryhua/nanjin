package com.github.chenharryhua.nanjin.common.logging

import cats.MonadThrow
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import io.circe.Encoder

abstract class Log[F[_]: MonadThrow]:
  /*
   * Log SPI
   */
  protected type M // middleman
  protected def create[S: Encoder](message: S, level: LogLevel, stackTrace: Option[Throwable]): F[M]
  protected def publish(event: M): F[Unit]
  protected def enabled(level: LogLevel): F[Boolean]

  /*
   * Log API
   */

  private def log[S: Encoder](
    message: => S,
    level: LogLevel,
    stackTrace: Option[Throwable]
  ): F[Unit] = {
    def process: F[Unit] = create[S](message, level, stackTrace).flatMap(publish).attempt.void
    enabled(level).ifM(process, ().pure[F])
  }

  final def error[S: Encoder](msg: => S): F[Unit] = log[S](msg, LogLevel.Error, None)
  final def error[S: Encoder](msg: => S, ex: Throwable): F[Unit] =
    log[S](msg, LogLevel.Error, Some(ex))

  final def warn[S: Encoder](msg: => S): F[Unit] = log[S](msg, LogLevel.Warn, None)
  final def warn[S: Encoder](msg: => S, ex: Throwable): F[Unit] =
    log[S](msg, LogLevel.Warn, Some(ex))

  final def good[S: Encoder](msg: => S): F[Unit] = log[S](msg, LogLevel.Good, None)
  final def info[S: Encoder](msg: => S): F[Unit] = log[S](msg, LogLevel.Info, None)

  final def debug[S: Encoder](msg: => S): F[Unit] = log[S](msg, LogLevel.Debug, None)
  final def debug[S: Encoder](msg: F[S]): F[Unit] =
    msg.attempt.flatMap {
      case Left(ex)     => log[String]("Debug Error", LogLevel.Debug, Some(ex))
      case Right(value) => log[S](value, LogLevel.Debug, None)
    }
end Log

object Log:
  def noop[F[_]: MonadThrow]: Log[F] = new Log[F] {
    private val unit: F[Unit] = ().pure[F]
    private val disabled: F[Boolean] = false.pure[F]

    override protected type M = Unit
    override protected def create[S: Encoder](
      message: S,
      level: LogLevel,
      stackTrace: Option[Throwable]): F[M] = unit
    override protected def publish(event: M): F[Unit] = unit
    override protected def enabled(level: LogLevel): F[Boolean] = disabled
  }
end Log
