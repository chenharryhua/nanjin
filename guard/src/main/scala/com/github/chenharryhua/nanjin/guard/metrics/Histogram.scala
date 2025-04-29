package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.effect.kernel.{Resource, Sync}
import cats.implicits.{catsSyntaxTuple2Semigroupal, toFunctorOps}
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.{utils, EnableConfig}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.HistogramKind
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, NJUnits}

trait Histogram[F[_]] extends KleisliLike[F, Long] {
  def update(num: Long): F[Unit]

  final def update(num: Int): F[Unit] = update(num.toLong)

  final override def run(num: Long): F[Unit] = update(num)
}

object Histogram {
  def noop[F[_]](implicit F: Applicative[F]): Histogram[F] =
    new Histogram[F] {
      override def update(num: Long): F[Unit] = F.unit
    }

  private class Impl[F[_]: Sync](
    private[this] val label: MetricLabel,
    private[this] val metricRegistry: metrics.MetricRegistry,
    private[this] val unit: MeasurementUnit,
    private[this] val reservoir: Option[metrics.Reservoir],
    private[this] val name: MetricName)
      extends Histogram[F] {

    private[this] val F = Sync[F]

    private[this] val histogram_name: String =
      MetricID(label, name, Category.Histogram(HistogramKind.Histogram, unit)).identifier

    private[this] val supplier: metrics.MetricRegistry.MetricSupplier[metrics.Histogram] = () =>
      reservoir match {
        case Some(value) => new metrics.Histogram(value)
        case None => new metrics.Histogram(new metrics.ExponentiallyDecayingReservoir) // default reservoir
      }

    private[this] lazy val histogram: metrics.Histogram =
      metricRegistry.histogram(histogram_name, supplier)

    override def update(num: Long): F[Unit] = F.delay(histogram.update(num))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(histogram_name)).void
  }

  final class Builder private[guard] (
    isEnabled: Boolean,
    unit: MeasurementUnit,
    reservoir: Option[metrics.Reservoir])
      extends EnableConfig[Builder] {

    def withUnit(f: NJUnits.type => MeasurementUnit): Builder =
      new Builder(isEnabled, f(NJUnits), reservoir)

    def withReservoir(reservoir: metrics.Reservoir): Builder =
      new Builder(isEnabled, unit, Some(reservoir))

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, unit, reservoir)

    private[guard] def build[F[_]](label: MetricLabel, name: String, metricRegistry: metrics.MetricRegistry)(
      implicit F: Sync[F]): Resource[F, Histogram[F]] =
      if (isEnabled) {
        Resource.make((F.monotonic, utils.randomUUID[F]).mapN { case (ts, unique) =>
          new Impl[F](
            label = label,
            metricRegistry = metricRegistry,
            unit = unit,
            reservoir = reservoir,
            name = MetricName(name, ts, unique))
        })(_.unregister)
      } else
        Resource.pure(noop[F])
  }
}
