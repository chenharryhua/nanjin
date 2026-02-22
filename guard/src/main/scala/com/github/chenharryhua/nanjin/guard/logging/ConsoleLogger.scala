package com.github.chenharryhua.nanjin.guard.logging

import cats.Monad
import cats.effect.kernel.Clock
import cats.effect.std.Console
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import org.typelevel.log4cats.{LoggerName, MessageLogger}

import java.time.ZoneId
import java.time.format.DateTimeFormatter

final private class ConsoleLogger[F[_]: Console: Clock: Monad](zoneId: ZoneId, loggerName: LoggerName)
    extends MessageLogger[F] {
  private[this] val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  private[this] def out(message: String, logLevel: String): F[Unit] =
    Clock[F].realTimeInstant.map(t => t.atZone(zoneId).toLocalDateTime.format(fmt)).flatMap { time =>
      Console[F].println(s"$time ${loggerName.value} $logLevel -- $message")
    }

  override def error(message: => String): F[Unit] = out(message, "Console Error")
  override def warn(message: => String): F[Unit] = out(message, "Console Warn")
  override def info(message: => String): F[Unit] = out(message, "Console Info")
  override def debug(message: => String): F[Unit] = out(message, "Console Debug")
  override def trace(message: => String): F[Unit] = out(message, "Console Trace")
}
