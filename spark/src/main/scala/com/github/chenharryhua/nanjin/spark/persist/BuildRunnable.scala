package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync

trait BuildRunnable[F[_]] {
  def run(implicit F: Sync[F]): F[Unit]
}
