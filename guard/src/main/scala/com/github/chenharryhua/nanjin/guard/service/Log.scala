package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Ref, Sync}
import cats.implicits.{catsSyntaxEq, catsSyntaxIfM, catsSyntaxPartialOrder, toFlatMapOps, toFunctorOps}
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Error
import io.circe.Encoder

sealed trait Log[F[_]] {
  def warn[S: Encoder](msg: S): F[Unit]
  def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def done[S: Encoder](msg: S): F[Unit]
  def info[S: Encoder](msg: S): F[Unit]

  def debug[S: Encoder](msg: S): F[Unit]
  def debug[S: Encoder](msg: => F[S]): F[Unit]

  def void[S](msg: S): F[Unit]
}

abstract private class LogImpl[F[_]](
  serviceParams: ServiceParams,
  eventLogger: EventLogger[F],
  alarmLevel: Ref[F, Option[AlarmLevel]]
)(implicit F: Sync[F])
    extends Log[F] {

  private def alarm[S: Encoder](msg: S, level: AlarmLevel, error: Option[Error]): F[Unit] =
    alarmLevel.get
      .map(_.exists(level >= _))
      .ifM(toServiceMessage(serviceParams, msg, level, error).flatMap(eventLogger.service_message), F.unit)

  override def warn[S: Encoder](msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Warn, None)

  override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Warn, Some(Error(ex)))

  override def info[S: Encoder](msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Info, None)

  override def done[S: Encoder](msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Done, None)

  override def debug[S: Encoder](msg: => F[S]): F[Unit] =
    alarmLevel.get
      .map(_.exists(_ === AlarmLevel.Debug))
      .ifM(
        F.attempt(msg).flatMap {
          case Left(ex) =>
            toServiceMessage(serviceParams, "Error", AlarmLevel.Debug, Some(Error(ex)))
              .flatMap(eventLogger.service_message)
          case Right(value) =>
            toServiceMessage(serviceParams, value, AlarmLevel.Debug, None)
              .flatMap(eventLogger.service_message)
        },
        F.unit
      )

  override def debug[S: Encoder](msg: S): F[Unit] =
    debug[S](F.pure(msg))

  override def void[S](msg: S): F[Unit] = F.unit
}
