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
  def soleInfo[S: Encoder](msg: S): F[Unit]
  def info[S: Encoder](msg: S): F[Unit]

  def soleDone[S: Encoder](msg: S): F[Unit]
  def done[S: Encoder](msg: S): F[Unit]

  def soleError[S: Encoder](msg: S): F[Unit]
  def error[S: Encoder](msg: S): F[Unit]

  def soleError[S: Encoder](ex: Throwable)(msg: S): F[Unit]
  def error[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def soleWarn[S: Encoder](msg: S): F[Unit]
  def warn[S: Encoder](msg: S): F[Unit]

  def soleWarn[S: Encoder](ex: Throwable)(msg: S): F[Unit]
  def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit]
}

abstract private class HeraldImpl[F[_]: Sync](
  serviceParams: ServiceParams,
  channel: Channel[F, Event],
  eventLogger: EventLogger[F],
  errorHistory: AtomicCell[F, CircularFifoQueue[ServiceMessage]]
) extends Herald[F] {

  private def alarm[S: Encoder](msg: S, level: AlarmLevel, error: Option[Error]): F[Unit] =
    create_service_message(serviceParams, msg, level, error).flatMap(m => channel.send(m)).void

  override def soleInfo[S: Encoder](msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Info, None)

  override def info[S: Encoder](msg: S): F[Unit] =
    eventLogger.info(serviceParams, msg) *> soleInfo(msg)

  override def soleDone[S: Encoder](msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Done, None)

  override def done[S: Encoder](msg: S): F[Unit] =
    eventLogger.done(serviceParams, msg) *> soleDone(msg)

  override def soleWarn[S: Encoder](msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Warn, None)

  override def warn[S: Encoder](msg: S): F[Unit] =
    eventLogger.warn(serviceParams, msg) *> soleWarn(msg)

  override def soleWarn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Warn, Some(Error(ex)))

  override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    eventLogger.warn(ex)(serviceParams, msg) *> soleWarn(ex)(msg)

  override def soleError[S: Encoder](msg: S): F[Unit] =
    for {
      evt <- create_service_message(serviceParams, msg, AlarmLevel.Error, None)
      _ <- errorHistory.modify(queue => (queue, queue.add(evt)))
      _ <- channel.send(evt)
    } yield ()

  override def error[S: Encoder](msg: S): F[Unit] =
    eventLogger.error(serviceParams, msg) *> soleError(msg)

  override def soleError[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    for {
      evt <- create_service_message(serviceParams, msg, AlarmLevel.Error, Error(ex).some)
      _ <- errorHistory.modify(queue => (queue, queue.add(evt)))
      _ <- channel.send(evt)
    } yield ()

  override def error[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    eventLogger.error(ex)(serviceParams, msg) *> soleError(ex)(msg)
}
