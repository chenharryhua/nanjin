package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Counter, Histogram, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.Digested
import com.github.chenharryhua.nanjin.guard.event.{MetricCategory, MetricName}
import io.circe.syntax.EncoderOps

final class NJHistogram[F[_]] private[guard] (
  digested: Digested,
  metricRegistry: MetricRegistry,
  isCounting: Boolean)(implicit F: Sync[F]) {
  private lazy val histogram: Histogram =
    metricRegistry.histogram(MetricName(digested, MetricCategory.Histogram).asJson.noSpaces)
  private lazy val counter: Counter =
    metricRegistry.counter(MetricName(digested, MetricCategory.HistogramCount).asJson.noSpaces)

  def withCounting: NJHistogram[F] = new NJHistogram[F](digested, metricRegistry, true)

  def unsafeUpdate(num: Long): Unit = {
    histogram.update(num)
    if (isCounting) counter.inc(num)
  }

  def update(num: Long): F[Unit] = F.delay(unsafeUpdate(num))
}
