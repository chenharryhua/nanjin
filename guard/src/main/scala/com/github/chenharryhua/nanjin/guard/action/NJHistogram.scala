package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Histogram, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.MetricName

final class NJHistogram[F[_]](metricName: MetricName, metricRegistry: MetricRegistry)(implicit F: Sync[F]) {
  private lazy val histo: Histogram = metricRegistry.histogram(histogramMRName(metricName))

  def unsafeUpdate(num: Long): Unit = histo.update(num)
  def update(num: Long): F[Unit]    = F.delay(unsafeUpdate(num))
}
