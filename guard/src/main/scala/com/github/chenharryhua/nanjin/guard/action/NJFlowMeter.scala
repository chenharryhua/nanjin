package com.github.chenharryhua.nanjin.guard.action

import cats.Applicative
import cats.data.Kleisli
import cats.effect.kernel.{Resource, Sync, Unique}
import cats.implicits.toFunctorOps
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.{CounterKind, HistogramKind, MeterKind}
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, NJUnits}

sealed trait NJFlowMeter[F[_]] {
  def unsafeUpdate(num: Long): Unit
  def update(num: Long): F[Unit]

  final def kleisli[A](f: A => Long): Kleisli[F, A, Unit] =
    Kleisli(update).local(f)
}

private class NJFlowMeterImpl[F[_]: Sync](
  private[this] val token: Unique.Token,
  private[this] val name: MetricName,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val unit: MeasurementUnit,
  private[this] val isCounting: Boolean,
  private[this] val reservoir: Option[Reservoir])
    extends NJFlowMeter[F] {

  private[this] val F = Sync[F]

  private[this] val histogram_name: String =
    MetricID(name, Category.Histogram(HistogramKind.FlowMeter, unit), token).identifier

  private[this] val meter_name: String =
    MetricID(name, Category.Meter(MeterKind.FlowMeter, unit), token).identifier

  private[this] val counter_name: String =
    MetricID(name, Category.Counter(CounterKind.FlowMeter, MetricTag(None)), token).identifier

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

  override def unsafeUpdate(num: Long): Unit = calculate(num)
  override def update(num: Long): F[Unit]    = F.delay(calculate(num))

  val unregister: F[Unit] = F.delay {
    metricRegistry.remove(meter_name)
    metricRegistry.remove(histogram_name)
    metricRegistry.remove(counter_name)
  }.void
}

object NJFlowMeter {
  def dummy[F[_]](implicit F: Applicative[F]): NJFlowMeter[F] =
    new NJFlowMeter[F] {
      override def unsafeUpdate(num: Long): Unit = ()
      override def update(num: Long): F[Unit]    = F.unit
    }

  final class Builder private[guard] (
    measurement: Measurement,
    unit: MeasurementUnit,
    isCounting: Boolean,
    reservoir: Option[Reservoir],
    isEnabled: Boolean)
      extends EnableConfig[Builder] {
    def withMeasurement(measurement: String): Builder =
      new Builder(Measurement(measurement), unit, isCounting, reservoir, isEnabled)

    def counted: Builder = new Builder(measurement, unit, isCounting = true, reservoir, isEnabled)

    def withUnit(f: NJUnits.type => MeasurementUnit): Builder =
      new Builder(measurement, f(NJUnits), isCounting, reservoir, isEnabled)

    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(measurement, unit, isCounting, Some(reservoir), isEnabled)

    def enable(value: Boolean): Builder =
      new Builder(measurement, unit, isCounting, reservoir, isEnabled = value)

    private[guard] def build[F[_]](
      name: String,
      metricRegistry: MetricRegistry,
      serviceParams: ServiceParams)(implicit F: Sync[F]): Resource[F, NJFlowMeter[F]] =
      if (isEnabled) {
        val metricName = MetricName(serviceParams, measurement, name)
        Resource.make(
          F.unique.map(new NJFlowMeterImpl[F](_, metricName, metricRegistry, unit, isCounting, reservoir)))(
          _.unregister)
      } else
        Resource.pure(dummy[F])
  }
}
