package com.github.chenharryhua.nanjin.guard.logging

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.flatMap.{catsSyntaxIfM, toFlatMapOps}
import cats.syntax.functor.toFunctorOps
import cats.syntax.order.catsSyntaxPartialOrder
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{Domain, StackTrace}
import io.circe.Encoder

trait Logger[F[_]] {
  def debug[S: Encoder](msg: S): F[Unit]
  def debug[S: Encoder](msg: => F[S]): F[Unit]

  def info[S: Encoder](msg: S): F[Unit]
  def done[S: Encoder](msg: S): F[Unit]

  def warn[S: Encoder](msg: S): F[Unit]
  def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def void[S](msg: S): F[Unit]
}

object Logger {
  def apply[F[_]: Sync](
    serviceParams: ServiceParams,
    domain: Domain,
    alarmLevel: Ref[F, Option[AlarmLevel]],
    logEvent: LogEvent[F]): Logger[F] =
    new LogImpl[F](serviceParams, domain, alarmLevel, logEvent)

  final private class LogImpl[F[_]](
    serviceParams: ServiceParams,
    domain: Domain,
    alarmLevel: Ref[F, Option[AlarmLevel]],
    logEvent: LogEvent[F])(implicit F: Sync[F])
      extends Logger[F] {
    private def log_service_message[S: Encoder](
      message: S,
      level: AlarmLevel,
      stackTrace: Option[StackTrace]): F[Unit] =
      alarmLevel.get
        .map(_.exists(_ <= level))
        .ifM(
          create_service_message[F, S](serviceParams, domain, message, level, stackTrace)
            .flatMap(logEvent.logEvent),
          F.unit)

    override def void[S](msg: S): F[Unit] = F.unit
    override def info[S: Encoder](msg: S): F[Unit] = log_service_message(msg, AlarmLevel.Info, None)
    override def done[S: Encoder](msg: S): F[Unit] = log_service_message(msg, AlarmLevel.Done, None)
    override def warn[S: Encoder](msg: S): F[Unit] = log_service_message(msg, AlarmLevel.Warn, None)
    override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
      log_service_message(msg, AlarmLevel.Warn, Some(StackTrace(ex)))

    override def debug[S: Encoder](msg: S): F[Unit] = log_service_message(msg, AlarmLevel.Debug, None)
    override def debug[S: Encoder](msg: => F[S]): F[Unit] = msg.flatMap(debug(_))
  }
}
