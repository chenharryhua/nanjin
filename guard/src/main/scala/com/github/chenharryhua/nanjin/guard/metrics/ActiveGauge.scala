package com.github.chenharryhua.nanjin.guard.metrics

trait ActiveGauge[F[_]] extends KleisliLike[F, Unit] {
  def deactivate: F[Unit]
  final override def run(a: Unit): F[Unit] = deactivate
}
