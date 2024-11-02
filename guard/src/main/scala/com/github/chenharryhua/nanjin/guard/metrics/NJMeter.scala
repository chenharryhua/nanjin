package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.effect.kernel.{Resource, Sync, Unique}
import cats.implicits.toFunctorOps
import com.codahale.metrics.{Meter, MetricRegistry}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.MeterKind
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, NJUnits}

sealed trait NJMeter[F[_]] extends KleisliLike[F, Long] {
  def run(num: Long): F[Unit]
  def update(num: Long): F[Unit]

  final def update(num: Int): F[Unit] = update(num.toLong)
}

object NJMeter {
  def dummy[F[_]](implicit F: Applicative[F]): NJMeter[F] =
    new NJMeter[F] {
      override def update(num: Long): F[Unit] = F.unit
      override def run(num: Long): F[Unit]    = F.unit
    }

  private class Impl[F[_]: Sync](
    private[this] val token: Unique.Token,
    private[this] val name: MetricName,
    private[this] val metricRegistry: MetricRegistry,
    private[this] val unit: MeasurementUnit,
    private[this] val tag: MetricTag)
      extends NJMeter[F] {

    private[this] val F = Sync[F]

    private[this] val meter_name: String =
      MetricID(name, tag, Category.Meter(MeterKind.Meter, unit), token).identifier

    private[this] lazy val meter: Meter = metricRegistry.meter(meter_name)

    override def run(num: Long): F[Unit]    = F.delay(meter.mark(num))
    override def update(num: Long): F[Unit] = F.delay(meter.mark(num))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(meter_name)).void
  }

  final class Builder private[guard] (isEnabled: Boolean, unit: MeasurementUnit)
      extends EnableConfig[Builder] {

    def withUnit(f: NJUnits.type => MeasurementUnit): Builder =
      new Builder(isEnabled, f(NJUnits))

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, unit)

    private[guard] def build[F[_]](metricName: MetricName, tag: MetricTag, metricRegistry: MetricRegistry)(
      implicit F: Sync[F]): Resource[F, NJMeter[F]] =
      if (isEnabled) {
        Resource.make(
          F.unique.map(token =>
            new Impl[F](
              token = token,
              name = metricName,
              metricRegistry = metricRegistry,
              unit = unit,
              tag = tag)))(_.unregister)
      } else
        Resource.pure(dummy[F])
  }
}
