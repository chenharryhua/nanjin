package com.github.chenharryhua.nanjin.guard.metrics

trait IdleGauge[F[_]] extends KleisliLike[F, Unit] {
  def wakeUp: F[Unit]

  final override def run(a: Unit): F[Unit] = wakeUp
}
