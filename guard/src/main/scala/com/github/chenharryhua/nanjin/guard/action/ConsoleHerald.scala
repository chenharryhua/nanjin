package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Ref, Sync}
import cats.effect.std.Console
import cats.implicits.{catsSyntaxEq, catsSyntaxIfM, catsSyntaxPartialOrder, toFlatMapOps, toFunctorOps}
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Error
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceMessage
import com.github.chenharryhua.nanjin.guard.translator.jsonHelper
import io.circe.Encoder

import java.time.format.DateTimeFormatter
import scala.Console as SConsole

trait ConsoleHerald[F[_]] {
  def error[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit]
  def error[S: Encoder](ex: Throwable)(msg: S)(implicit cns: Console[F]): F[Unit]

  def warn[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit]
  def warn[S: Encoder](ex: Throwable)(msg: S)(implicit cns: Console[F]): F[Unit]

  def info[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit]
  def done[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit]
  def debug[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit]
  def debug[S: Encoder](msg: => F[S])(implicit cns: Console[F]): F[Unit]
}

object ConsoleHerald {
  private[guard] class Impl[F[_]](
    serviceParams: ServiceParams,
    alarmLevel: Ref[F, AlarmLevel]
  )(implicit F: Sync[F])
      extends ConsoleHerald[F] {

    private val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    private def toText(sm: ServiceMessage): String = {
      val color = sm.level match {
        case AlarmLevel.Error => SConsole.RED + "Console ERROR" + SConsole.RESET
        case AlarmLevel.Warn  => SConsole.YELLOW + "Console Warn" + SConsole.RESET
        case AlarmLevel.Done  => SConsole.GREEN + "Console Done" + SConsole.RESET
        case AlarmLevel.Info  => SConsole.CYAN + "Console Info" + SConsole.RESET
        case AlarmLevel.Debug => SConsole.BLUE + "Console Debug" + SConsole.RESET
      }
      val msg = jsonHelper.json_service_message(sm).noSpaces
      s"${sm.timestamp.format(fmt)} $color - $msg"
    }

    private def toServiceMessage[S: Encoder](msg: S, level: AlarmLevel, error: Option[Error]): F[String] =
      serviceParams.zonedNow.map(ts =>
        toText(
          ServiceMessage(
            serviceParams = serviceParams,
            timestamp = ts,
            level = level,
            error = error,
            message = Encoder[S].apply(msg))))

    private def logMessage[S: Encoder](msg: S, level: AlarmLevel, error: Option[Error])(implicit
      cns: Console[F]): F[Unit] =
      alarmLevel.get.map(level >= _).ifM(toServiceMessage(msg, level, error).flatMap(cns.println), F.unit)

    override def error[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit] =
      logMessage(msg, AlarmLevel.Error, None)

    override def warn[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit] =
      logMessage(msg, AlarmLevel.Warn, None)

    override def info[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit] =
      logMessage(msg, AlarmLevel.Info, None)

    override def done[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit] =
      logMessage(msg, AlarmLevel.Done, None)

    override def debug[S: Encoder](msg: S)(implicit cns: Console[F]): F[Unit] =
      logMessage(msg, AlarmLevel.Debug, None)

    override def error[S: Encoder](ex: Throwable)(msg: S)(implicit cns: Console[F]): F[Unit] =
      logMessage(msg, AlarmLevel.Error, Some(Error(ex)))

    override def warn[S: Encoder](ex: Throwable)(msg: S)(implicit cns: Console[F]): F[Unit] =
      logMessage(msg, AlarmLevel.Warn, Some(Error(ex)))

    override def debug[S: Encoder](msg: => F[S])(implicit cns: Console[F]): F[Unit] =
      alarmLevel.get
        .map(_ === AlarmLevel.Debug)
        .ifM(
          F.attempt(msg).flatMap {
            case Left(ex) =>
              toServiceMessage("Debug Error", AlarmLevel.Debug, Some(Error(ex))).flatMap(cns.println)
            case Right(value) =>
              toServiceMessage(value, AlarmLevel.Debug, None).flatMap(cns.println)
          },
          F.unit
        )
  }
}
