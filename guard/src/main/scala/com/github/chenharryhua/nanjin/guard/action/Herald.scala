package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import cats.effect.std.Console
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceMessage
import com.github.chenharryhua.nanjin.guard.event.{Event, NJError}
import com.github.chenharryhua.nanjin.guard.translator.{jsonHelper, textHelper, ColorScheme}
import fs2.concurrent.Channel
import io.circe.Encoder

import java.time.format.DateTimeFormatter

trait Herald[F[_]] {
  def error[S: Encoder](msg: S): F[Unit]
  def error[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def warn[S: Encoder](msg: S): F[Unit]
  def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def info[S: Encoder](msg: S): F[Unit]
  def done[S: Encoder](msg: S): F[Unit]

  def consoleError[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit]
  def consoleError[S: Encoder](ex: Throwable)(msg: S)(implicit cns: Console[F]): F[Unit]

  def consoleWarn[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit]
  def consoleWarn[S: Encoder](ex: Throwable)(msg: S)(implicit cns: Console[F]): F[Unit]

  def consoleInfo[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit]
  def consoleDone[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit]
}

object Herald {

  private[guard] class Impl[F[_]](
    serviceParams: ServiceParams,
    channel: Channel[F, Event]
  )(implicit F: Sync[F])
      extends Herald[F] {

    private def toServiceMessage[S: Encoder](
      msg: S,
      level: AlarmLevel,
      error: Option[NJError]): F[ServiceMessage] =
      serviceParams.zonedNow.map(ts =>
        ServiceMessage(
          serviceParams = serviceParams,
          timestamp = ts,
          level = level,
          error = error,
          message = Encoder[S].apply(msg)))

    private def alarm[S: Encoder](msg: S, level: AlarmLevel, error: Option[NJError]): F[Unit] =
      toServiceMessage(msg, level, error).flatMap(channel.send).void

    override def error[S: Encoder](msg: S): F[Unit] = alarm(msg, AlarmLevel.Error, None)
    override def warn[S: Encoder](msg: S): F[Unit]  = alarm(msg, AlarmLevel.Warn, None)
    override def info[S: Encoder](msg: S): F[Unit]  = alarm(msg, AlarmLevel.Info, None)
    override def done[S: Encoder](msg: S): F[Unit]  = alarm(msg, AlarmLevel.Done, None)

    override def error[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
      alarm(msg, AlarmLevel.Error, Some(NJError(ex)))
    override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
      alarm(msg, AlarmLevel.Warn, Some(NJError(ex)))

    // console

    private val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    private def toText(sm: ServiceMessage): String = {
      val color = ColorScheme.decorate(sm).run(textHelper.consoleColor).value
      val msg   = jsonHelper.json_service_message(sm).noSpaces
      s"${sm.timestamp.format(fmt)} Console $color - $msg"
    }

    override def consoleError[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit] =
      toServiceMessage(msg, AlarmLevel.Error, None).flatMap(m => cns.println(toText(m)))
    override def consoleWarn[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit] =
      toServiceMessage(msg, AlarmLevel.Warn, None).flatMap(m => cns.println(toText(m)))
    override def consoleInfo[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit] =
      toServiceMessage(msg, AlarmLevel.Info, None).flatMap(m => cns.println(toText(m)))
    override def consoleDone[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit] =
      toServiceMessage(msg, AlarmLevel.Done, None).flatMap(m => cns.println(toText(m)))

    override def consoleError[S: Encoder](ex: Throwable)(msg: S)(implicit cns: Console[F]): F[Unit] =
      toServiceMessage(msg, AlarmLevel.Error, Some(NJError(ex))).flatMap(m => cns.println(toText(m)))
    override def consoleWarn[S: Encoder](ex: Throwable)(msg: S)(implicit cns: Console[F]): F[Unit] =
      toServiceMessage(msg, AlarmLevel.Warn, Some(NJError(ex))).flatMap(m => cns.println(toText(m)))
  }
}
