package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Counter, Histogram, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.MeasurementName
import com.github.chenharryhua.nanjin.guard.event.{MetricCategory, MetricID}
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

final class NJHistogram[F[_]] private[guard] (
  name: MeasurementName,
  unit: StandardUnit,
  metricRegistry: MetricRegistry,
  isCounting: Boolean)(implicit F: Sync[F]) {
  private lazy val histogram: Histogram =
    metricRegistry.histogram(MetricID(name, MetricCategory.Histogram(unit)).asJson.noSpaces)
  private lazy val counter: Counter =
    metricRegistry.counter(MetricID(name, MetricCategory.HistogramCounter).asJson.noSpaces)

  def withCounting: NJHistogram[F] = new NJHistogram[F](name, unit, metricRegistry, true)

  def unsafeUpdate(num: Long): Unit = {
    histogram.update(num)
    if (isCounting) counter.inc(1) // number of updates
  }

  def update(num: Long): F[Unit] = F.delay(unsafeUpdate(num))
}
