package com.github.chenharryhua.nanjin.guard.action

import cats.data.Kleisli
import cats.effect.kernel.{Resource, Sync, Unique}
import cats.implicits.{catsSyntaxHash, toFunctorOps}
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.{CounterKind, HistogramKind, MeterKind}
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, NJUnits}

final class NJFlowMeter[F[_]: Sync] private (
  private[this] val token: Unique.Token,
  private[this] val name: MetricName,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val unit: MeasurementUnit,
  private[this] val isCounting: Boolean,
  private[this] val reservoir: Option[Reservoir]) {

  private[this] val F = Sync[F]

  private[this] val histogram_name: String =
    MetricID(name, Category.Histogram(HistogramKind.FlowMeter, unit), token.hash).identifier

  private[this] val meter_name: String =
    MetricID(name, Category.Meter(MeterKind.FlowMeter, unit), token.hash).identifier

  private[this] val counter_name: String =
    MetricID(name, Category.Counter(CounterKind.FlowMeter), token.hash).identifier

  private[this] val supplier: MetricRegistry.MetricSupplier[Histogram] = () =>
    reservoir match {
      case Some(value) => new Histogram(value)
      case None        => new Histogram(new ExponentiallyDecayingReservoir) // default reservoir
    }

  private[this] lazy val meter: Meter         = metricRegistry.meter(meter_name)
  private[this] lazy val histogram: Histogram = metricRegistry.histogram(histogram_name, supplier)
  private[this] lazy val counter: Counter     = metricRegistry.counter(counter_name)

  private[this] val calculate: Long => Unit =
    if (isCounting) { (num: Long) =>
      histogram.update(num)
      meter.mark(num)
      counter.inc(1)
    } else { (num: Long) =>
      histogram.update(num)
      meter.mark(num)
    }

  def unsafeUpdate(num: Long): Unit = calculate(num)
  def update(num: Long): F[Unit]    = F.delay(calculate(num))

  def kleisli[A](f: A => Long): Kleisli[F, A, Unit] = Kleisli(update).local(f)

  private val unregister: F[Unit] = F.delay {
    metricRegistry.remove(meter_name)
    metricRegistry.remove(histogram_name)
    metricRegistry.remove(counter_name)
  }.void
}

object NJFlowMeter {

  final class Builder private[guard] (
    measurement: Measurement,
    unit: MeasurementUnit,
    isCounting: Boolean,
    reservoir: Option[Reservoir]) {
    def withMeasurement(measurement: String): Builder =
      new Builder(Measurement(measurement), unit, isCounting, reservoir)

    def counted: Builder = new Builder(measurement, unit, true, reservoir)

    def withUnit(f: NJUnits.type => MeasurementUnit): Builder =
      new Builder(measurement, f(NJUnits), isCounting, reservoir)

    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(measurement, unit, isCounting, Some(reservoir))

    private[guard] def build[F[_]: Sync](
      name: String,
      metricRegistry: MetricRegistry,
      serviceParams: ServiceParams): Resource[F, NJFlowMeter[F]] = {
      val metricName = MetricName(serviceParams, measurement, name)
      Resource.make(
        Sync[F].unique.map(new NJFlowMeter[F](_, metricName, metricRegistry, unit, isCounting, reservoir)))(
        _.unregister)
    }
  }
}
