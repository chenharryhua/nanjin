package com.github.chenharryhua.nanjin.guard.metrics

trait NJIdleGauge[F[_]] extends KleisliLike[F, Unit] {
  def mark: F[Unit]

  final override def run(a: Unit): F[Unit] = mark
}
