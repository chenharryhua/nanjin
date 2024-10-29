package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import cats.effect.std.Console
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, MetricName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ServiceMessage
import com.github.chenharryhua.nanjin.guard.event.{textColor, NJEvent}
import fs2.concurrent.Channel
import io.circe.{Encoder, Json}
import org.typelevel.log4cats.Logger

sealed trait NJMessenger[F[_]] {
  def error[S: Encoder](msg: S): F[Unit]
  def warn[S: Encoder](msg: S): F[Unit]
  def info[S: Encoder](msg: S): F[Unit]
  def done[S: Encoder](msg: S): F[Unit]

  def logError[S: Encoder](msg: S)(implicit F: Logger[F]): F[Unit]
  def logWarn[S: Encoder](msg: S)(implicit F: Logger[F]): F[Unit]
  def logInfo[S: Encoder](msg: S)(implicit F: Logger[F]): F[Unit]
  def logDone[S: Encoder](msg: S)(implicit F: Logger[F]): F[Unit]

  def consoleError[S: Encoder](msg: S)(implicit F: Console[F]): F[Unit]
  def consoleWarn[S: Encoder](msg: S)(implicit F: Console[F]): F[Unit]
  def consoleInfo[S: Encoder](msg: S)(implicit F: Console[F]): F[Unit]
  def consoleDone[S: Encoder](msg: S)(implicit F: Console[F]): F[Unit]
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

    override def logError[S: Encoder](msg: S)(implicit F: Logger[F]): F[Unit] =
      F.error(Encoder[S].apply(msg).noSpaces)
    override def logWarn[S: Encoder](msg: S)(implicit F: Logger[F]): F[Unit] =
      F.warn(Encoder[S].apply(msg).noSpaces)
    override def logInfo[S: Encoder](msg: S)(implicit F: Logger[F]): F[Unit] =
      F.info(Encoder[S].apply(msg).noSpaces)
    override def logDone[S: Encoder](msg: S)(implicit F: Logger[F]): F[Unit] =
      F.info(Encoder[S].apply(msg).noSpaces)

    override def consoleError[S: Encoder](msg: S)(implicit F: Console[F]): F[Unit] =
      F.println(s"""|${textColor.errorTerm} ${metricName.name}
                    |${Encoder[S].apply(msg).spaces2}""".stripMargin)
    override def consoleWarn[S: Encoder](msg: S)(implicit F: Console[F]): F[Unit] =
      F.println(s"""|${textColor.warnTerm} ${metricName.name}
                    |${Encoder[S].apply(msg).spaces2}""".stripMargin)
    override def consoleInfo[S: Encoder](msg: S)(implicit F: Console[F]): F[Unit] =
      F.println(s"""|${textColor.infoTerm} ${metricName.name}
                    |${Encoder[S].apply(msg).spaces2}""".stripMargin)
    override def consoleDone[S: Encoder](msg: S)(implicit F: Console[F]): F[Unit] =
      F.println(s"""|${textColor.goodTerm} ${metricName.name}
                    |${Encoder[S].apply(msg).spaces2}""".stripMargin)
  }
}
