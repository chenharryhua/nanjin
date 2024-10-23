package com.github.chenharryhua.nanjin.guard.action

import cats.Applicative
import cats.data.Kleisli
import cats.effect.kernel.{Resource, Sync, Unique}
import cats.implicits.toFunctorOps
import com.codahale.metrics.{Meter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.MeterKind
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
  private[this] val unit: MeasurementUnit,
  private[this] val tag: MetricTag)
    extends NJMeter[F] {

  private[this] val F = Sync[F]

  private[this] val meter_name: String =
    MetricID(name, Category.Meter(MeterKind.Meter, tag, unit), token).identifier

  private[this] lazy val meter: Meter = metricRegistry.meter(meter_name)

  override def unsafeUpdate(num: Long): Unit = meter.mark(num)
  override def update(num: Long): F[Unit]    = F.delay(meter.mark(num))

  val unregister: F[Unit] = F.delay(metricRegistry.remove(meter_name)).void
}

object NJMeter {
  def dummy[F[_]](implicit F: Applicative[F]): NJMeter[F] =
    new NJMeter[F] {
      override def unsafeUpdate(num: Long): Unit = ()
      override def update(num: Long): F[Unit]    = F.unit
    }

  final class Builder private[guard] (
    measurement: Measurement,
    unit: MeasurementUnit,
    isEnabled: Boolean,
    tag: MetricTag)
      extends MetricBuilder[Builder] {

    def withUnit(f: NJUnits.type => MeasurementUnit): Builder =
      new Builder(measurement, f(NJUnits), isEnabled, tag)

    override def withMeasurement(measurement: String): Builder =
      new Builder(Measurement(measurement), unit, isEnabled, tag)

    override def enable(value: Boolean): Builder =
      new Builder(measurement, unit, value, tag)

    override def withTag(tag: String): Builder =
      new Builder(measurement, unit, isEnabled, MetricTag(tag))

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
              unit = unit,
              tag = tag)))(_.unregister)
      } else
        Resource.pure(dummy[F])
  }
}
