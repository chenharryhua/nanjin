package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.MetricName

final class NJHistogram[F[_]: Sync](metricName: MetricName, metricRegistry: MetricRegistry) {
  private val name: String = histogramMRName(metricName)

  def unsafeUpdate(n: Long): Unit = metricRegistry.histogram(name).update(n)
  def update(n: Long): F[Unit]    = Sync[F].delay(unsafeUpdate(n))
}
