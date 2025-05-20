package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Ref, Sync}
import cats.implicits.{catsSyntaxEq, catsSyntaxIfM, catsSyntaxPartialOrder, toFlatMapOps, toFunctorOps}
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Error
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceMessage
import com.github.chenharryhua.nanjin.guard.translator.jsonHelper
import io.circe.Encoder

import java.time.format.DateTimeFormatter
import scala.Console as SConsole

sealed trait ConsoleHerald[F[_]] {
  def warn[S: Encoder](msg: S): F[Unit]
  def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def done[S: Encoder](msg: S): F[Unit]
  def info[S: Encoder](msg: S): F[Unit]

  def debug[S: Encoder](msg: S): F[Unit]
  def debug[S: Encoder](msg: => F[S]): F[Unit]

  def void[S](msg: S): F[Unit]
}

abstract private class ConsoleHeraldImpl[F[_]](
  serviceParams: ServiceParams,
  alarmLevel: Ref[F, AlarmLevel]
)(implicit F: Sync[F])
    extends ConsoleHerald[F] {

  private val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  private def toText(sm: ServiceMessage): String = {
    val color = sm.level match {
      case AlarmLevel.Disable => ""
      case AlarmLevel.Error   => SConsole.RED + "Console Error" + SConsole.RESET
      case AlarmLevel.Warn    => SConsole.YELLOW + "Console Warn" + SConsole.RESET
      case AlarmLevel.Done    => SConsole.GREEN + "Console Success" + SConsole.RESET
      case AlarmLevel.Info    => SConsole.CYAN + "Console Info" + SConsole.RESET
      case AlarmLevel.Debug   => SConsole.BLUE + "Console Debug" + SConsole.RESET
    }
    val msg = jsonHelper.json_service_message(sm).noSpaces
    s"${sm.timestamp.format(fmt)} $color - $msg"
  }

  private def screenPrintln(str: String): F[Unit] = Sync[F].blocking(println(str))

  private def alarm[S: Encoder](msg: S, level: AlarmLevel, error: Option[Error]): F[Unit] =
    alarmLevel.get
      .map(level >= _)
      .ifM(toServiceMessage(serviceParams, msg, level, error).map(toText).flatMap(screenPrintln), F.unit)

  override def warn[S: Encoder](msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Warn, None)

  override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Warn, Some(Error(ex)))

  override def info[S: Encoder](msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Info, None)

  override def done[S: Encoder](msg: S): F[Unit] =
    alarm(msg, AlarmLevel.Done, None)

  override def debug[S: Encoder](msg: => F[S]): F[Unit] =
    alarmLevel.get
      .map(_ === AlarmLevel.Debug)
      .ifM(
        F.attempt(msg).flatMap {
          case Left(ex) =>
            toServiceMessage(serviceParams, "Error", AlarmLevel.Debug, Some(Error(ex)))
              .map(toText)
              .flatMap(screenPrintln)
          case Right(value) =>
            toServiceMessage(serviceParams, value, AlarmLevel.Debug, None).map(toText).flatMap(screenPrintln)
        },
        F.unit
      )

  override def debug[S: Encoder](msg: S): F[Unit] =
    debug[S](F.pure(msg))

  override def void[S](msg: S): F[Unit] = F.unit
}
