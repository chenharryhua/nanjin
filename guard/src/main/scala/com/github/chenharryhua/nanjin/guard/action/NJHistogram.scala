package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Counter, Histogram, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{CountAction, DigestedName}

final class NJHistogram[F[_]] private[guard] (
  metricName: DigestedName,
  metricRegistry: MetricRegistry,
  isCounting: CountAction)(implicit F: Sync[F]) {
  private lazy val histo: Histogram = metricRegistry.histogram(histogramMRName(metricName))
  private lazy val counter: Counter = metricRegistry.counter(counterMRName(metricName, asError = false))

  def withCounting: NJHistogram[F] = new NJHistogram[F](metricName, metricRegistry, CountAction.Yes)

  def unsafeUpdate(num: Long): Unit = {
    histo.update(num)
    if (isCounting.value) counter.inc(num)
  }
  def update(num: Long): F[Unit] = F.delay(unsafeUpdate(num))
}
