package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{Error, Event}
import fs2.concurrent.Channel
import io.circe.Encoder

trait Herald[F[_]] {
  def error[S: Encoder](msg: S): F[Unit]
  def error[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def warn[S: Encoder](msg: S): F[Unit]
  def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def info[S: Encoder](msg: S): F[Unit]
  def done[S: Encoder](msg: S): F[Unit]

}

object Herald {

  private[guard] class Impl[F[_]: Sync](
    serviceParams: ServiceParams,
    channel: Channel[F, Event]
  ) extends Herald[F] {

    private def alarm[S: Encoder](msg: S, level: AlarmLevel, error: Option[Error]): F[Unit] =
      toServiceMessage(serviceParams, msg, level, error).flatMap(channel.send).void

    override def error[S: Encoder](msg: S): F[Unit] = alarm(msg, AlarmLevel.Error, None)
    override def warn[S: Encoder](msg: S): F[Unit]  = alarm(msg, AlarmLevel.Warn, None)
    override def info[S: Encoder](msg: S): F[Unit]  = alarm(msg, AlarmLevel.Info, None)
    override def done[S: Encoder](msg: S): F[Unit]  = alarm(msg, AlarmLevel.Done, None)

    override def error[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
      alarm(msg, AlarmLevel.Error, Some(Error(ex)))
    override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
      alarm(msg, AlarmLevel.Warn, Some(Error(ex)))

  }
}
