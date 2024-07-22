package com.github.chenharryhua.nanjin.guard.action

import cats.data.Kleisli
import cats.effect.kernel.{Resource, Sync, Unique}
import cats.implicits.toFunctorOps
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.{CounterKind, HistogramKind}
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, NJUnits}

sealed trait NJHistogram[F[_]] {
  def unsafeUpdate(num: Long): Unit
  def update(num: Long): F[Unit]
  final def kleisli[A](f: A => Long): Kleisli[F, A, Unit] =
    Kleisli(update).local(f)
}

private class NJHistogramImpl[F[_]: Sync](
  private[this] val token: Unique.Token,
  private[this] val name: MetricName,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val unit: MeasurementUnit,
  private[this] val isCounting: Boolean,
  private[this] val reservoir: Option[Reservoir])
    extends NJHistogram[F] {

  private[this] val F = Sync[F]

  private[this] val histogram_name: String =
    MetricID(name, Category.Histogram(HistogramKind.Histogram, unit), token).identifier
  private[this] val counter_name: String =
    MetricID(name, Category.Counter(CounterKind.Histogram), token).identifier

  private[this] val supplier: MetricRegistry.MetricSupplier[Histogram] = () =>
    reservoir match {
      case Some(value) => new Histogram(value)
      case None        => new Histogram(new ExponentiallyDecayingReservoir) // default reservoir
    }

  private[this] lazy val histogram: Histogram = metricRegistry.histogram(histogram_name, supplier)
  private[this] lazy val counter: Counter     = metricRegistry.counter(counter_name)

  private[this] val calculate: Long => Unit =
    if (isCounting) { (num: Long) =>
      histogram.update(num)
      counter.inc(1)
    } else (num: Long) => histogram.update(num)

  override def unsafeUpdate(num: Long): Unit = calculate(num)
  override def update(num: Long): F[Unit]    = F.delay(calculate(num))

  val unregister: F[Unit] = F.delay {
    metricRegistry.remove(histogram_name)
    metricRegistry.remove(counter_name)
  }.void
}

object NJHistogram {

  final class Builder private[guard] (
    measurement: Measurement,
    unit: MeasurementUnit,
    isCounting: Boolean,
    reservoir: Option[Reservoir],
    isEnabled: Boolean)
      extends EnableConfig[Builder] {
    def withMeasurement(measurement: String): Builder =
      new Builder(Measurement(measurement), unit, isCounting, reservoir, isEnabled)

    def counted: Builder = new Builder(measurement, unit, true, reservoir, isEnabled)

    def withUnit(f: NJUnits.type => MeasurementUnit): Builder =
      new Builder(measurement, f(NJUnits), isCounting, reservoir, isEnabled)

    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(measurement, unit, isCounting, Some(reservoir), isEnabled)

    def enable(value: Boolean): Builder =
      new Builder(measurement, unit, isCounting, reservoir, value)

    private[guard] def build[F[_]](
      name: String,
      metricRegistry: MetricRegistry,
      serviceParams: ServiceParams)(implicit F: Sync[F]): Resource[F, NJHistogram[F]] =
      if (isEnabled) {
        val metricName = MetricName(serviceParams, measurement, name)
        Resource.make(
          F.unique.map(token =>
            new NJHistogramImpl[F](
              token = token,
              name = metricName,
              metricRegistry = metricRegistry,
              unit = unit,
              isCounting = isCounting,
              reservoir = reservoir)))(_.unregister)
      } else
        Resource.pure(new NJHistogram[F] {
          override def unsafeUpdate(num: Long): Unit = ()
          override def update(num: Long): F[Unit]    = F.unit
        })
  }
}
