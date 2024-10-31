package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import cats.effect.std.Console
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ServiceMessage
import com.github.chenharryhua.nanjin.guard.event.{textColor, NJEvent}
import fs2.concurrent.Channel
import io.circe.{Encoder, Json}

import java.time.format.DateTimeFormatter

sealed trait NJHerald[F[_]] {
  def error[S: Encoder](msg: S): F[Unit]
  def warn[S: Encoder](msg: S): F[Unit]
  def info[S: Encoder](msg: S): F[Unit]
  def good[S: Encoder](msg: S): F[Unit]

  def consoleError[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit]
  def consoleWarn[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit]
  def consoleInfo[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit]
  def consoleGood[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit]
}

object NJHerald {

  private[guard] class Impl[F[_]](
    serviceParams: ServiceParams,
    channel: Channel[F, NJEvent]
  )(implicit F: Sync[F])
      extends NJHerald[F] {

    private def toServiceMessage[S: Encoder](msg: S, level: AlarmLevel): F[ServiceMessage] =
      serviceParams.zonedNow.map(ts =>
        ServiceMessage(
          serviceParams = serviceParams,
          timestamp = ts,
          level = level,
          message = Encoder[S].apply(msg)))

    private def alarm[S: Encoder](msg: S, level: AlarmLevel): F[Unit] =
      toServiceMessage(msg, level).flatMap(channel.send).void

    override def error[S: Encoder](msg: S): F[Unit] = alarm(msg, AlarmLevel.Error)
    override def warn[S: Encoder](msg: S): F[Unit]  = alarm(msg, AlarmLevel.Warn)
    override def info[S: Encoder](msg: S): F[Unit]  = alarm(msg, AlarmLevel.Info)
    override def good[S: Encoder](msg: S): F[Unit]  = alarm(msg, AlarmLevel.Good)

    // console

    private val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    private def toText(sm: ServiceMessage): String = {
      import com.github.chenharryhua.nanjin.guard.translator.jsonHelper
      val msg = Json.obj(
        jsonHelper.serviceName(serviceParams),
        jsonHelper.serviceId(serviceParams),
        jsonHelper.alarmMessage(sm)
      )
      val color = sm.level match {
        case AlarmLevel.Error => textColor.errorTerm
        case AlarmLevel.Warn  => textColor.warnTerm
        case AlarmLevel.Info  => textColor.infoTerm
        case AlarmLevel.Good  => textColor.goodTerm
      }
      s"${sm.timestamp.format(fmt)} Console $color - ${msg.noSpaces}"
    }

    override def consoleError[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit] =
      toServiceMessage(msg, AlarmLevel.Error).flatMap(m => cns.println(toText(m)))
    override def consoleWarn[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit] =
      toServiceMessage(msg, AlarmLevel.Warn).flatMap(m => cns.println(toText(m)))
    override def consoleInfo[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit] =
      toServiceMessage(msg, AlarmLevel.Info).flatMap(m => cns.println(toText(m)))
    override def consoleGood[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit] =
      toServiceMessage(msg, AlarmLevel.Good).flatMap(m => cns.println(toText(m)))
  }
}
