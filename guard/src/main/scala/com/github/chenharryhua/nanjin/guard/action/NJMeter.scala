package com.github.chenharryhua.nanjin.guard.action

import cats.data.Kleisli
import cats.effect.kernel.{Resource, Sync, Unique}
import cats.implicits.toFunctorOps
import com.codahale.metrics.{Counter, Meter, MetricRegistry}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.{CounterKind, MeterKind}
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, NJUnits}

sealed trait NJMeter[F[_]] {
  def unsafeUpdate(num: Long): Unit
  def update(num: Long): F[Unit]

  final def kleisli[A](f: A => Long): Kleisli[F, A, Unit] =
    Kleisli(update).local(f)
}

private class NJMeterImpl[F[_]: Sync](
  private[this] val token: Unique.Token,
  private[this] val name: MetricName,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val isCounting: Boolean,
  private[this] val unit: MeasurementUnit)
    extends NJMeter[F] {

  private[this] val F = Sync[F]

  private[this] val meter_name: String =
    MetricID(name, Category.Meter(MeterKind.Meter, unit), token).identifier

  private[this] val counter_name: String =
    MetricID(name, Category.Counter(CounterKind.Meter, MetricTag(None)), token).identifier

  private[this] lazy val meter: Meter     = metricRegistry.meter(meter_name)
  private[this] lazy val counter: Counter = metricRegistry.counter(counter_name)

  private[this] val calculate: Long => Unit =
    if (isCounting) { (num: Long) =>
      meter.mark(num)
      counter.inc(num)
    } else (num: Long) => meter.mark(num)

  override def unsafeUpdate(num: Long): Unit = calculate(num)
  override def update(num: Long): F[Unit]    = F.delay(calculate(num))

  val unregister: F[Unit] = F.delay {
    metricRegistry.remove(meter_name)
    metricRegistry.remove(counter_name)
  }.void
}

object NJMeter {

  final class Builder private[guard] (
    measurement: Measurement,
    unit: MeasurementUnit,
    isCounting: Boolean,
    isEnabled: Boolean)
      extends EnableConfig[Builder] {
    def withMeasurement(measurement: String): Builder =
      new Builder(Measurement(measurement), unit, isCounting, isEnabled)

    def counted: Builder = new Builder(measurement, unit, true, isEnabled)

    def withUnit(f: NJUnits.type => MeasurementUnit): Builder =
      new Builder(measurement, f(NJUnits), isCounting, isEnabled)

    def enable(value: Boolean): Builder =
      new Builder(measurement, unit, isCounting, value)

    private[guard] def build[F[_]](
      name: String,
      metricRegistry: MetricRegistry,
      serviceParams: ServiceParams)(implicit F: Sync[F]): Resource[F, NJMeter[F]] =
      if (isEnabled) {
        val metricName = MetricName(serviceParams, measurement, name)
        Resource.make(
          F.unique.map(token =>
            new NJMeterImpl[F](
              token = token,
              name = metricName,
              metricRegistry = metricRegistry,
              isCounting = isCounting,
              unit = unit)))(_.unregister)
      } else
        Resource.pure(new NJMeter[F] {
          override def unsafeUpdate(num: Long): Unit = ()
          override def update(num: Long): F[Unit]    = F.unit
        })
  }
}
