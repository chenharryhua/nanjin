package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.effect.kernel.{Resource, Sync}
import cats.implicits.{catsSyntaxTuple2Semigroupal, toFunctorOps}
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.{utils, EnableConfig}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.MeterKind
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, NJUnits}

trait Meter[F[_]] extends KleisliLike[F, Long] {
  def mark(num: Long): F[Unit]

  final def mark(num: Int): F[Unit] = mark(num.toLong)

  final override def run(num: Long): F[Unit] = mark(num)
}

object Meter {
  def noop[F[_]](implicit F: Applicative[F]): Meter[F] =
    new Meter[F] {
      override def mark(num: Long): F[Unit] = F.unit
    }

  private class Impl[F[_]: Sync](
    private[this] val label: MetricLabel,
    private[this] val metricRegistry: metrics.MetricRegistry,
    private[this] val unit: MeasurementUnit,
    private[this] val name: MetricName)
      extends Meter[F] {

    private[this] val F = Sync[F]

    private[this] val meter_name: String =
      MetricID(label, name, Category.Meter(MeterKind.Meter, unit)).identifier

    private[this] lazy val meter: metrics.Meter = metricRegistry.meter(meter_name)

    override def mark(num: Long): F[Unit] = F.delay(meter.mark(num))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(meter_name)).void
  }

  val initial: Builder = new Builder(isEnabled = true, unit = NJUnits.COUNT)

  final class Builder private[guard] (isEnabled: Boolean, unit: MeasurementUnit)
      extends EnableConfig[Builder] {

    def withUnit(f: NJUnits.type => MeasurementUnit): Builder =
      new Builder(isEnabled, f(NJUnits))

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, unit)

    private[guard] def build[F[_]](label: MetricLabel, name: String, metricRegistry: metrics.MetricRegistry)(
      implicit F: Sync[F]): Resource[F, Meter[F]] =
      if (isEnabled) {
        Resource.make((F.monotonic, utils.randomUUID[F]).mapN { case (ts, unique) =>
          new Impl[F](
            label = label,
            metricRegistry = metricRegistry,
            unit = unit,
            name = MetricName(name, ts, unique))
        })(_.unregister)
      } else
        Resource.pure(noop[F])
  }
}
