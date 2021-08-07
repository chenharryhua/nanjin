package com.github.chenharryhua.nanjin.common

import cats.effect.kernel.{Resource, Sync}
import com.codahale.metrics.MetricRegistry

object metrics {

  @FunctionalInterface
  trait NJMetricsReporter {
    def start[F[_]](registry: MetricRegistry)(implicit F: Sync[F]): Resource[F, Unit]
  }
}
