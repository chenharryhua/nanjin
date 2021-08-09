package com.github.chenharryhua.nanjin.common

import cats.effect.kernel.{Async, Resource}
import com.codahale.metrics.MetricRegistry

object metrics {

  @FunctionalInterface
  trait NJMetricReporter {
    def start[F[_]](registry: MetricRegistry)(implicit F: Async[F]): F[Nothing]
  }
}
