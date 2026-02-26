package com.github.chenharryhua.nanjin.guard.logging

import cats.effect.MonadCancel
import cats.syntax.applicativeError.catsSyntaxApplicativeError
import cats.syntax.flatMap.{catsSyntaxIfM, toFlatMapOps}
import cats.syntax.functor.toFunctorOps
import cats.{MonadError, Semigroup}
import com.github.chenharryhua.nanjin.guard.config.AlarmLevel
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import com.github.chenharryhua.nanjin.guard.event.StackTrace
import io.circe.Encoder

/** Logging and Event System Overview
  *
  * This system separates **event reporting**, **persistent logging**, and **broadcasting** into distinct,
  * composable components.
  *
  * Components:
  *
  *   - `LogSink`: Low-level writer that persists events to disk or console.
  *   - `Logger`: Wraps `LogSink` to provide domain-aware logging with alarm levels. Writes events to disk.
  *   - `Herald`: Broadcasts `ReportedEvent`s to channels, service buses, or other consumers capable of
  *     decoding `Event`.
  *   - Combined log: Users can combine `Logger` and `Herald` using `|+|` to get both persistence and
  *     broadcasting in a single `Log[F]`.
  *
  * Event Flow:
  *
  * {{{
  *                 ┌─────────────┐
  *                 │   Agent     │
  *                 │  (Facade)   │
  *                 └─────┬───────┘
  *                       │
  *          ┌────────────┴─────────────┐
  *          │                          │
  *    ┌─────────────┐            ┌─────────────┐
  *    │   Logger    │            │   Herald    │
  *    │ (writes to  │            │ (broadcasts │
  *    │  disk via   │            │  to channels│
  *    │  LogSink)   │            │  / service  │
  *    │             │            │  bus)       │
  *    └─────┬───────┘            └─────┬───────┘
  *          │                          │
  *          └───────────┬──────────────┘
  *                      │
  *                ┌─────────────┐
  *                │ CombinedLog │
  *                │  Logger |+| │
  *                │   Herald    │
  *                └─────────────┘
  * }}}
  *
  * Key Points:
  *
  *   - Flexible Broadcast: `Herald` can send events to any consumer that understands the `Event` format.
  *   - Persistence and Delivery: Combining `Logger` and `Herald` ensures events are both persisted to disk
  *     and delivered to interested channels.
  *   - Domain-Aware Logging: `Logger` respects domain context and alarm levels.
  *
  * Usage:
  *
  * {{{
  * val log: Log[F] = agent.logger |+| agent.herald
  * }}}
  */
abstract class Log[F[_]](implicit F: MonadError[F, Throwable]) {
  // create must be sink-independent
  private[logging] def create[S: Encoder](
    message: S,
    level: AlarmLevel,
    stackTrace: Option[StackTrace]): F[ReportedEvent]

  private[logging] def publish(event: ReportedEvent): F[Unit]

  private[logging] def enabled(level: AlarmLevel): F[Boolean]

  private def log[S: Encoder](
    message: => S,
    level: AlarmLevel,
    stackTrace: Option[StackTrace]
  ): F[Unit] =
    enabled(level).ifM(create(message, level, stackTrace).flatMap(publish).attempt.void, F.unit)

  final def error[S: Encoder](msg: => S): F[Unit] = log[S](msg, AlarmLevel.Error, None)
  final def error[S: Encoder](msg: => S, ex: Throwable): F[Unit] =
    log[S](msg, AlarmLevel.Error, Some(StackTrace(ex)))

  final def warn[S: Encoder](msg: => S): F[Unit] = log[S](msg, AlarmLevel.Warn, None)
  final def warn[S: Encoder](msg: => S, ex: Throwable): F[Unit] =
    log[S](msg, AlarmLevel.Warn, Some(StackTrace(ex)))

  final def good[S: Encoder](msg: => S): F[Unit] = log[S](msg, AlarmLevel.Good, None)
  final def info[S: Encoder](msg: => S): F[Unit] = log[S](msg, AlarmLevel.Info, None)

  final def debug[S: Encoder](msg: => S): F[Unit] = log[S](msg, AlarmLevel.Debug, None)
  final def debug[S: Encoder](msg: F[S]): F[Unit] =
    msg.attempt.flatMap {
      case Left(ex)     => log[String]("Debug Error", AlarmLevel.Debug, Some(StackTrace(ex)))
      case Right(value) => log[S](value, AlarmLevel.Debug, None)
    }
}

object Log {

  implicit def semigroupLog[F[_]](implicit F: MonadCancel[F, Throwable]): Semigroup[Log[F]] =
    new Semigroup[Log[F]] {
      override def combine(x: Log[F], y: Log[F]): Log[F] = new Log[F] {
        override private[logging] def create[S: Encoder](
          message: S,
          level: AlarmLevel,
          stackTrace: Option[StackTrace]): F[ReportedEvent] = x.create(message, level, stackTrace)

        override private[logging] def publish(event: ReportedEvent): F[Unit] =
          F.uncancelable { _ =>
            for {
              _ <- x.enabled(event.level).ifM(x.publish(event), F.unit)
              _ <- y.enabled(event.level).ifM(y.publish(event), F.unit)
            } yield ()
          }

        override private[logging] def enabled(level: AlarmLevel): F[Boolean] =
          x.enabled(level).flatMap {
            case true  => F.pure(true)
            case false => y.enabled(level)
          }
      }
    }
}
