package com.github.chenharryhua.nanjin.guard

import scala.concurrent.duration.FiniteDuration

trait Notifiable[F[_]] {
  def info(msg: String): F[Unit]
  def warn: F[Unit]
  def error: F[Unit]
}

final case class AlertEveryNRetries(value: Int) extends AnyVal
final case class MaximumRetries(value: Long) extends AnyVal
final case class RetryInterval(value: FiniteDuration) extends AnyVal
