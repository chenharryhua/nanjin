package com.github.chenharryhua.nanjin.guard.logging

import cats.effect.kernel.Sync
import cats.effect.std.AtomicCell
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.option.catsSyntaxOptionId
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceMessage
import com.github.chenharryhua.nanjin.guard.event.{Domain, Event, StackTrace}
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

object Herald {
  def apply[F[_]: Sync](
    serviceParams: ServiceParams,
    domain: Domain,
    channel: Channel[F, Event],
    logEvent: LogEvent[F],
    errorHistory: AtomicCell[F, CircularFifoQueue[ServiceMessage]]): Herald[F] =
    new HeraldImpl[F](serviceParams, domain, channel, logEvent, errorHistory)

  final private class HeraldImpl[F[_]: Sync](
    serviceParams: ServiceParams,
    domain: Domain,
    channel: Channel[F, Event],
    logEvent: LogEvent[F],
    errorHistory: AtomicCell[F, CircularFifoQueue[ServiceMessage]]
  ) extends Herald[F] {

    override def info[S: Encoder](msg: S): F[Unit] =
      for {
        evt <- create_service_message[F, S](serviceParams, domain, msg, AlarmLevel.Info, None)
        _ <- logEvent.logEvent(evt)
        _ <- channel.send(evt)
      } yield ()

    override def done[S: Encoder](msg: S): F[Unit] =
      for {
        evt <- create_service_message[F, S](serviceParams, domain, msg, AlarmLevel.Done, None)
        _ <- logEvent.logEvent(evt)
        _ <- channel.send(evt)
      } yield ()

    override def warn[S: Encoder](msg: S): F[Unit] =
      for {
        evt <- create_service_message[F, S](serviceParams, domain, msg, AlarmLevel.Warn, None)
        _ <- logEvent.logEvent(evt)
        _ <- channel.send(evt)
      } yield ()

    override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
      for {
        evt <- create_service_message[F, S](serviceParams, domain, msg, AlarmLevel.Warn, Some(StackTrace(ex)))
        _ <- logEvent.logEvent(evt)
        _ <- channel.send(evt)
      } yield ()

    override def error[S: Encoder](msg: S): F[Unit] =
      for {
        evt <- create_service_message[F, S](serviceParams, domain, msg, AlarmLevel.Error, None)
        _ <- errorHistory.modify(queue => (queue, queue.add(evt)))
        _ <- logEvent.logEvent(evt)
        _ <- channel.send(evt)
      } yield ()

    override def error[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
      for {
        evt <- create_service_message[F, S](serviceParams, domain, msg, AlarmLevel.Error, StackTrace(ex).some)
        _ <- errorHistory.modify(queue => (queue, queue.add(evt)))
        _ <- logEvent.logEvent(evt)
        _ <- channel.send(evt)
      } yield ()

  }
}
