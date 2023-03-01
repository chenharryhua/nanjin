package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Counter, Histogram, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.Digested
import com.github.chenharryhua.nanjin.guard.event.{MetricCategory, MetricID}
import io.circe.syntax.EncoderOps

final class NJHistogram[F[_]] private[guard] (
  digested: Digested,
  unitOfMeasure: String,
  metricRegistry: MetricRegistry,
  isCounting: Boolean)(implicit F: Sync[F]) {
  private lazy val histogram: Histogram =
    metricRegistry.histogram(MetricID(digested, MetricCategory.Histogram(unitOfMeasure)).asJson.noSpaces)
  private lazy val counter: Counter =
    metricRegistry.counter(MetricID(digested, MetricCategory.HistogramCounter).asJson.noSpaces)

  def withCounting: NJHistogram[F] = new NJHistogram[F](digested, unitOfMeasure, metricRegistry, true)

  def unsafeUpdate(num: Long): Unit = {
    histogram.update(num)
    if (isCounting) counter.inc(1) // number of updates
  }

  def update(num: Long): F[Unit] = F.delay(unsafeUpdate(num))
}
