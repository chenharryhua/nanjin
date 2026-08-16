package com.github.chenharryhua.nanjin.guard.metrics.api.gauges

/** Effectful two-sided balance that transfers values between a source and target. */
trait BalanceGauge[F[_], A]:
  /** Move `num` from the configured source side to the target side. */
  def forward(num: A): F[Unit]

  /** Move `num` from the configured target side back to the source side. */
  def backward(num: A): F[Unit]
end BalanceGauge
