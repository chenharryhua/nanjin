package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Sync
import cats.effect.std.AtomicCell
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceMessage
import com.github.chenharryhua.nanjin.guard.event.{Error, Event}
import fs2.concurrent.Channel
import io.circe.Encoder
import org.apache.commons.collections4.queue.CircularFifoQueue

sealed trait Herald[F[_]] {
  def info[S: Encoder](msg: S): F[Unit]
  def infoBoth[S: Encoder](msg: S): F[Unit]

  def done[S: Encoder](msg: S): F[Unit]
  def doneBoth[S: Encoder](msg: S): F[Unit]

  def error[S: Encoder](msg: S): F[Unit]
  def errorBoth[S: Encoder](msg: S): F[Unit]

  def error[S: Encoder](ex: Throwable)(msg: S): F[Unit]
  def errorBoth[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def warn[S: Encoder](msg: S): F[Unit]
  def warnBoth[S: Encoder](msg: S): F[Unit]

  def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit]
  def warnBoth[S: Encoder](ex: Throwable)(msg: S): F[Unit]
}

abstract private class HeraldImpl[F[_]: Sync](
  serviceParams: ServiceParams,
  channel: Channel[F, Event],
  eventLogger: EventLogger[F],
  errorHistory: AtomicCell[F, CircularFifoQueue[ServiceMessage]]
) extends Herald[F] {

  private def alarm[S: Encoder](msg: S, level: AlarmLevel, error: Option[Error]): F[Unit] =
    create_service_message(serviceParams, msg, level, error).flatMap(m => channel.send(m)).void

  override def info[S: Encoder](msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Info, None)
  override def infoBoth[S: Encoder](msg: S): F[Unit] =
    info(msg) *> eventLogger.info(serviceParams, msg)

  override def done[S: Encoder](msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Done, None)
  override def doneBoth[S: Encoder](msg: S): F[Unit] =
    done(msg) *> eventLogger.done(serviceParams, msg)

  override def warn[S: Encoder](msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Warn, None)

  override def warnBoth[S: Encoder](msg: S): F[Unit] =
    warn(msg) >> eventLogger.warn(serviceParams, msg)

  override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Warn, Some(Error(ex)))
  override def warnBoth[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    warn(ex)(msg) *> eventLogger.warn(ex)(serviceParams, msg)

  override def error[S: Encoder](msg: S): F[Unit] =
    for {
      evt <- create_service_message(serviceParams, msg, AlarmLevel.Error, None)
      _ <- errorHistory.modify(queue => (queue, queue.add(evt)))
      _ <- channel.send(evt)
    } yield ()

  override def errorBoth[S: Encoder](msg: S): F[Unit] =
    error(msg) *> eventLogger.error(serviceParams, msg)

  override def error[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    for {
      evt <- create_service_message(serviceParams, msg, AlarmLevel.Error, Error(ex).some)
      _ <- errorHistory.modify(queue => (queue, queue.add(evt)))
      _ <- channel.send(evt)
    } yield ()

  override def errorBoth[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    error(ex)(msg) *> eventLogger.error(ex)(serviceParams, msg)
}
