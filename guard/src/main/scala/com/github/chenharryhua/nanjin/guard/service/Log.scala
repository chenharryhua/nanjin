package com.github.chenharryhua.nanjin.guard.service

import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import io.circe.Encoder

sealed trait Log[F[_]] {

  def debug[S: Encoder](msg: S): F[Unit]
  def debug[S: Encoder](msg: => F[S]): F[Unit]

  def info[S: Encoder](msg: S): F[Unit]
  def done[S: Encoder](msg: S): F[Unit]

  def warn[S: Encoder](msg: S): F[Unit]
  def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def void[S](msg: S): F[Unit]
}

abstract private class LogImpl[F[_]](
  serviceParams: ServiceParams,
  eventLogger: EventLogger[F]
) extends Log[F] {

  override def warn[S: Encoder](msg: S): F[Unit] =
    eventLogger.warn[S](serviceParams, msg)

  override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    eventLogger.warn[S](ex)(serviceParams, msg)

  override def info[S: Encoder](msg: S): F[Unit] =
    eventLogger.info[S](serviceParams, msg)

  override def done[S: Encoder](msg: S): F[Unit] =
    eventLogger.done[S](serviceParams, msg)

  override def void[S](msg: S): F[Unit] =
    eventLogger.void

  override def debug[S: Encoder](msg: => F[S]): F[Unit] =
    eventLogger.debug[S](serviceParams, msg)

  override def debug[S: Encoder](msg: S): F[Unit] =
    eventLogger.debug[S](serviceParams, msg)
}
