package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Counter, Histogram, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.Digested

final class NJHistogram[F[_]] private[guard] (
  digested: Digested,
  metricRegistry: MetricRegistry,
  isCounting: Boolean)(implicit F: Sync[F]) {
  private lazy val histo: Histogram = metricRegistry.histogram(histogramMRName(digested))
  private lazy val counter: Counter = metricRegistry.counter(counterMRName(digested, asError = false))

  def withCounting: NJHistogram[F] = new NJHistogram[F](digested, metricRegistry, true)

  def unsafeUpdate(num: Long): Unit = {
    histo.update(num)
    if (isCounting) counter.inc(num)
  }

  def update(num: Long): F[Unit] = F.delay(unsafeUpdate(num))
}
