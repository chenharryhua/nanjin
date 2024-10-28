package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, MetricName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ServiceMessage
import fs2.concurrent.Channel
import io.circe.{Encoder, Json}

sealed trait NJMessenger[F[_]] {
  def error[S: Encoder](msg: S): F[Unit]
  def warn[S: Encoder](msg: S): F[Unit]
  def info[S: Encoder](msg: S): F[Unit]
  def done[S: Encoder](msg: S): F[Unit]
}

object NJMessenger {

  private[guard] class Impl[F[_]](
    metricName: MetricName,
    serviceParams: ServiceParams,
    channel: Channel[F, NJEvent]
  )(implicit F: Sync[F])
      extends NJMessenger[F] {
    private[this] def alarm(msg: Json, level: AlarmLevel): F[Unit] =
      for {
        ts <- serviceParams.zonedNow
        _ <- channel.send(
          ServiceMessage(
            metricName = metricName,
            timestamp = ts,
            serviceParams = serviceParams,
            level = level,
            message = msg))
      } yield ()

    override def error[S: Encoder](msg: S): F[Unit] =
      alarm(Encoder[S].apply(msg), AlarmLevel.Error)

    override def warn[S: Encoder](msg: S): F[Unit] =
      alarm(Encoder[S].apply(msg), AlarmLevel.Warn)

    override def info[S: Encoder](msg: S): F[Unit] =
      alarm(Encoder[S].apply(msg), AlarmLevel.Info)

    override def done[S: Encoder](msg: S): F[Unit] =
      alarm(Encoder[S].apply(msg), AlarmLevel.Done)
  }
}
