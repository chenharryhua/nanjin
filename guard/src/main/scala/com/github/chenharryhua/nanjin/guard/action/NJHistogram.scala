package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import cats.implicits.toFunctorOps
import com.codahale.metrics.{Counter, Histogram, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{
  Category,
  CounterKind,
  HistogramKind,
  MetricID,
  MetricName
}
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

final class NJHistogram[F[_]] private[guard] (
  name: MetricName,
  metricRegistry: MetricRegistry,
  unit: StandardUnit,
  isCounting: Boolean)(implicit F: Sync[F]) {

  private val histogramName: String =
    MetricID(name, Category.Histogram(HistogramKind.Dropwizard, unit)).asJson.noSpaces
  private val counterName: String =
    MetricID(name, Category.Counter(CounterKind.HistoCounter)).asJson.noSpaces

  private lazy val histogram: Histogram = metricRegistry.histogram(histogramName)
  private lazy val counter: Counter     = metricRegistry.counter(counterName)

  private[guard] def unregister: F[Unit] = F.blocking {
    metricRegistry.remove(histogramName)
    metricRegistry.remove(counterName)
  }.void

  def withCounting: NJHistogram[F] = new NJHistogram[F](name, metricRegistry, unit, true)

  def unsafeUpdate(num: Long): Unit = {
    histogram.update(num)
    if (isCounting) counter.inc()
  }

  def update(num: Long): F[Unit] = F.delay(unsafeUpdate(num))
}
