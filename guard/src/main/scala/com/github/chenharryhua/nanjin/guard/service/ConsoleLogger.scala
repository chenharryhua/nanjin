package com.github.chenharryhua.nanjin.guard.service

import cats.Monad
import cats.effect.kernel.Clock
import cats.effect.std.Console
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import org.typelevel.log4cats.MessageLogger

import java.time.ZoneId
import java.time.format.DateTimeFormatter
import scala.io.AnsiColor

final private class ConsoleLogger[F[_]: Console: Clock: Monad](zoneId: ZoneId) extends MessageLogger[F] {
  private[this] val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  private[this] def out(message: String, logLevel: String): F[Unit] =
    Clock[F].realTimeInstant.map(t => t.atZone(zoneId).toLocalDateTime.format(fmt)).flatMap { time =>
      Console[F].println(s"$time $logLevel -- $message")
    }

  override def error(message: => String): F[Unit] = out(message, "Console Error")
  override def warn(message: => String): F[Unit] = out(message, "Console Warn")
  override def info(message: => String): F[Unit] = out(message, "Console Info")
  override def debug(message: => String): F[Unit] = out(message, "Console Debug")
  override def trace(message: => String): F[Unit] = out(message, "Console Trace")
}

sealed private trait LogColor {
  def done: String => String
  def info: String => String
  def warn: String => String
  def error: String => String
  def debug: String => String
}

private object LogColor {
  private def colorize(code: String)(name: String): String = code + name + AnsiColor.RESET

  val standard: LogColor = new LogColor {
    override val done: String => String = colorize(AnsiColor.GREEN)
    override val info: String => String = colorize(AnsiColor.CYAN)
    override val warn: String => String = colorize(AnsiColor.YELLOW)
    override val error: String => String = colorize(AnsiColor.RED)
    override val debug: String => String = colorize(AnsiColor.MAGENTA)
  }

  val none: LogColor = new LogColor {
    override val done: String => String = identity
    override val info: String => String = identity
    override val warn: String => String = identity
    override val error: String => String = identity
    override val debug: String => String = identity
  }
}
