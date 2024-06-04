package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync

trait BuildRunnable {
  def run[F[_]](implicit F: Sync[F]): F[Unit]
}
