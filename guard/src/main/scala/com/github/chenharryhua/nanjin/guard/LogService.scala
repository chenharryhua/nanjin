package com.github.chenharryhua.nanjin.guard

import cats.effect.Sync
import org.log4s.Logger

import scala.concurrent.duration.FiniteDuration

final case class AlertEveryNRetries(value: Int) extends AnyVal
final case class MaximumRetries(value: Long) extends AnyVal
final case class RetryInterval(value: FiniteDuration) extends AnyVal

trait LogService[F[_]] {
  def info(msg: String): F[Unit]
  def warn(msg: String, ex: Throwable): F[Unit]
  def error(msg: String, ex: Throwable): F[Unit]
  def limitedRetryError(ex: LimitedRetryState): F[Unit]
  def retryForeverError(ex: RetryForeverState): F[Unit]
}

final class NJLog[F[_]](logger: Logger)(implicit F: Sync[F]) extends LogService[F] {
  override def info(msg: String): F[Unit] = F.blocking(logger.info(msg))

  override def warn(msg: String, ex: Throwable): F[Unit] = F.blocking(logger.warn(ex)(msg))

  override def error(msg: String, ex: Throwable): F[Unit] = F.blocking(logger.error(ex)(msg))

  override def limitedRetryError(ex: LimitedRetryState): F[Unit] = F.blocking(logger.error(ex.err)("error"))

  override def retryForeverError(ex: RetryForeverState): F[Unit] = F.blocking(logger.error(ex.err)("error"))

}
