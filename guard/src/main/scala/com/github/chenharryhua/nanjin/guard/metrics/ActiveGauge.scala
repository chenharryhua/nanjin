package com.github.chenharryhua.nanjin.guard.metrics

trait ActiveGauge[F[_]] {
  def deactivate: F[Unit]
  final def run(a: Unit): F[Unit] = deactivate
}
