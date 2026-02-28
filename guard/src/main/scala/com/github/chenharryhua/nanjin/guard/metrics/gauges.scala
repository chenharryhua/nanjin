package com.github.chenharryhua.nanjin.guard.metrics

trait BalanceGauge[F[_], A] {
  def forward(num: A): F[Unit]
  def backward(num: A): F[Unit]
}

trait ActiveGauge[F[_]] {
  def deactivate: F[Unit]
}

trait IdleGauge[F[_]] {
  def wakeUp: F[Unit]
}
