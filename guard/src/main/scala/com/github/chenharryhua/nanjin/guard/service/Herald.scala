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

  def done[S: Encoder](msg: S): F[Unit]

  def error[S: Encoder](msg: S): F[Unit]
  def error[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def warn[S: Encoder](msg: S): F[Unit]
  def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit]
}

abstract private class HeraldImpl[F[_]: Sync](
  channel: Channel[F, Event],
  eventLogger: EventLogger[F],
  errorHistory: AtomicCell[F, CircularFifoQueue[ServiceMessage]]
) extends Herald[F] {
  private val serviceParams: ServiceParams = eventLogger.serviceParams

  private def alarm[S: Encoder](msg: S, level: AlarmLevel, error: Option[Error]): F[Unit] =
    serviceMessage(serviceParams, msg, level, error).flatMap(m => channel.send(m)).void

  override def info[S: Encoder](msg: S): F[Unit] =
    eventLogger.info(msg) *> alarm(msg, AlarmLevel.Info, None)

  override def done[S: Encoder](msg: S): F[Unit] =
    eventLogger.done(msg) *> alarm(msg, AlarmLevel.Done, None)

  override def warn[S: Encoder](msg: S): F[Unit] =
    eventLogger.warn(msg) *> alarm(msg, AlarmLevel.Warn, None)

  override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    eventLogger.warn(ex)(msg) *> alarm(msg, AlarmLevel.Warn, Some(Error(ex)))

  override def error[S: Encoder](msg: S): F[Unit] =
    for {
      evt <- serviceMessage(serviceParams, msg, AlarmLevel.Error, None)
      _ <- errorHistory.modify(queue => (queue, queue.add(evt)))
      _ <- eventLogger.error(msg)
      _ <- channel.send(evt)
    } yield ()

  override def error[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    for {
      evt <- serviceMessage(serviceParams, msg, AlarmLevel.Error, Error(ex).some)
      _ <- errorHistory.modify(queue => (queue, queue.add(evt)))
      _ <- eventLogger.error(ex)(msg)
      _ <- channel.send(evt)
    } yield ()

}
