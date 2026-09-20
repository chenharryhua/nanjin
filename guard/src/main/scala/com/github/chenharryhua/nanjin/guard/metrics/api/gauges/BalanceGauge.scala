package com.github.chenharryhua.nanjin.guard.metrics.api.gauges

import cats.Applicative
import cats.syntax.applicative.given

/** Effectful two-sided balance that transfers values between a source and target. */
trait BalanceGauge[F[_], A]:
  /** Move `num` from the configured source side to the target side. */
  def forward(num: A): F[Unit]

  /** Move `num` from the configured target side back to the source side. */
  def backward(num: A): F[Unit]
end BalanceGauge

object BalanceGauge:
  def noop[F[_]: Applicative, A]: BalanceGauge[F, A] = new BalanceGauge[F, A] {
    override def forward(num: A): F[Unit] = ().pure
    override def backward(num: A): F[Unit] = ().pure
  }
end BalanceGauge
