package com.github.chenharryhua.nanjin.guard.metrics

trait IdleGauge[F[_]] {
  def wakeUp: F[Unit]

  final def run(a: Unit): F[Unit] = wakeUp
}
