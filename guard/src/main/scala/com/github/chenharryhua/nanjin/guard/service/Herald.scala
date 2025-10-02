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

  override def info[S: Encoder](msg: S): F[Unit] =
    for {
      evt <- serviceMessage[F, S](serviceParams, msg, AlarmLevel.Info, None)
      _ <- eventLogger.logServiceMessage(evt)
      _ <- channel.send(evt)
    } yield ()

  override def done[S: Encoder](msg: S): F[Unit] =
    for {
      evt <- serviceMessage[F, S](serviceParams, msg, AlarmLevel.Done, None)
      _ <- eventLogger.logServiceMessage(evt)
      _ <- channel.send(evt)
    } yield ()

  override def warn[S: Encoder](msg: S): F[Unit] =
    for {
      evt <- serviceMessage[F, S](serviceParams, msg, AlarmLevel.Warn, None)
      _ <- eventLogger.logServiceMessage(evt)
      _ <- channel.send(evt)
    } yield ()

  override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    for {
      evt <- serviceMessage[F, S](serviceParams, msg, AlarmLevel.Warn, Some(Error(ex)))
      _ <- eventLogger.logServiceMessage(evt)
      _ <- channel.send(evt)
    } yield ()

  override def error[S: Encoder](msg: S): F[Unit] =
    for {
      evt <- serviceMessage[F, S](serviceParams, msg, AlarmLevel.Error, None)
      _ <- errorHistory.modify(queue => (queue, queue.add(evt)))
      _ <- eventLogger.logServiceMessage(evt)
      _ <- channel.send(evt)
    } yield ()

  override def error[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    for {
      evt <- serviceMessage[F, S](serviceParams, msg, AlarmLevel.Error, Error(ex).some)
      _ <- errorHistory.modify(queue => (queue, queue.add(evt)))
      _ <- eventLogger.logServiceMessage(evt)
      _ <- channel.send(evt)
    } yield ()

}
