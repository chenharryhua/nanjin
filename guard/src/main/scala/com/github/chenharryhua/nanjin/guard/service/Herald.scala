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
  def error[S: Encoder](msg: S): F[Unit]
  def error[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def warn[S: Encoder](msg: S): F[Unit]
  def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def done[S: Encoder](msg: S): F[Unit]
  def info[S: Encoder](msg: S): F[Unit]
}

abstract private class HeraldImpl[F[_]: Sync](
  serviceParams: ServiceParams,
  channel: Channel[F, Event],
  errorHistory: AtomicCell[F, CircularFifoQueue[ServiceMessage]]
) extends Herald[F] {

  private def alarm[S: Encoder](msg: S, level: AlarmLevel, error: Option[Error]): F[Unit] =
    toServiceMessage(serviceParams, msg, level, error).flatMap(channel.send).void

  override def warn[S: Encoder](msg: S): F[Unit] = alarm(msg, AlarmLevel.Warn, None)
  override def info[S: Encoder](msg: S): F[Unit] = alarm(msg, AlarmLevel.Info, None)
  override def done[S: Encoder](msg: S): F[Unit] = alarm(msg, AlarmLevel.Done, None)

  override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Warn, Some(Error(ex)))

  override def error[S: Encoder](msg: S): F[Unit] =
    for {
      msg <- toServiceMessage(serviceParams, msg, AlarmLevel.Error, None)
      _ <- errorHistory.modify(queue => (queue, queue.add(msg)))
      _ <- channel.send(msg)
    } yield ()

  override def error[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    for {
      msg <- toServiceMessage(
        serviceParams,
        msg,
        AlarmLevel.Error,
        Error(ex).some
      )
      _ <- errorHistory.modify(queue => (queue, queue.add(msg)))
      _ <- channel.send(msg)
    } yield ()
}
