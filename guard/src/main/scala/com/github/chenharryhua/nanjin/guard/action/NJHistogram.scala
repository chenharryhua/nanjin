package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import cats.implicits.toFunctorOps
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.guard.config.{
  Category,
  CounterKind,
  HistogramKind,
  MetricID,
  MetricName
}
import com.github.chenharryhua.nanjin.guard.event.MeasurementUnit

final class NJHistogram[F[_]] private[guard] (
  name: MetricName,
  metricRegistry: MetricRegistry,
  unit: MeasurementUnit,
  isCounting: Boolean,
  reservoir: Option[Reservoir])(implicit F: Sync[F]) {

  private val histogramName: String =
    MetricID(name, Category.Histogram(HistogramKind.Dropwizard, unit)).identifier
  private val counterName: String =
    MetricID(name, Category.Counter(CounterKind.HistoCounter)).identifier

  private lazy val supplier: MetricRegistry.MetricSupplier[Histogram] = () =>
    reservoir match {
      case Some(value) => new Histogram(value)
      case None        => new Histogram(new ExponentiallyDecayingReservoir) // default reservoir
    }

  private lazy val histogram: Histogram = metricRegistry.histogram(histogramName, supplier)
  private lazy val counter: Counter     = metricRegistry.counter(counterName)

  private[guard] def unregister: F[Unit] = F.blocking {
    metricRegistry.remove(histogramName)
    metricRegistry.remove(counterName)
  }.void

  def withReservoir(reservoir: Reservoir): NJHistogram[F] =
    new NJHistogram[F](name, metricRegistry, unit, isCounting, Some(reservoir))

  def counted: NJHistogram[F] = new NJHistogram[F](name, metricRegistry, unit, true, reservoir)

  def unsafeUpdate(num: Long): Unit = {
    histogram.update(num)
    if (isCounting) counter.inc()
  }

  def update(num: Long): F[Unit] = F.delay(unsafeUpdate(num))
}
