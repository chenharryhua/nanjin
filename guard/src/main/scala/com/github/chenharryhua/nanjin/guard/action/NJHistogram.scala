package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.MetricName

final class NJHistogram[F[_]](metricName: MetricName, metricRegistry: MetricRegistry)(implicit F: Sync[F]) {
  private val name: String = histogramMRName(metricName)

  def unsafeUpdate(n: Long): Unit = metricRegistry.histogram(name).update(n)
  def update(n: Long): F[Unit]    = F.delay(unsafeUpdate(n))
}
