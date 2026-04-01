package com.github.chenharryhua.nanjin.guard.metrics.gauges

trait BalanceGauge[F[_], A]:
  def forward(num: A): F[Unit]
  def backward(num: A): F[Unit]
