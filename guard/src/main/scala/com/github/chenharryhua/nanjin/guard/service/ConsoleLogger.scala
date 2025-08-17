package com.github.chenharryhua.nanjin.guard.service

import cats.Monad
import cats.effect.kernel.Clock
import cats.effect.std.Console
import cats.implicits.{toFlatMapOps, toFunctorOps}
import org.typelevel.log4cats.Logger

import java.time.ZoneId
import java.time.format.DateTimeFormatter

final private class ConsoleLogger[F[_]: Console: Clock: Monad](zoneId: ZoneId) extends Logger[F] {
  private[this] val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  private[this] def out(message: String, logLevel: String): F[Unit] =
    Clock[F].realTimeInstant.map(t => t.atZone(zoneId).toLocalDateTime.format(fmt)).flatMap { time =>
      Console[F].println(s"$time $logLevel -- $message")
    }

  override def error(t: Throwable)(message: => String): F[Unit] = out(message, "ERROR")
  override def warn(t: Throwable)(message: => String): F[Unit] = out(message, "WARN")
  override def info(t: Throwable)(message: => String): F[Unit] = out(message, "INFO")
  override def debug(t: Throwable)(message: => String): F[Unit] = out(message, "DEBUG")
  override def trace(t: Throwable)(message: => String): F[Unit] = out(message, "TRACE")
  override def error(message: => String): F[Unit] = out(message, "ERROR")
  override def warn(message: => String): F[Unit] = out(message, "WARN")
  override def info(message: => String): F[Unit] = out(message, "INFO")
  override def debug(message: => String): F[Unit] = out(message, "DEBUG")
  override def trace(message: => String): F[Unit] = out(message, "TRACE")
}
