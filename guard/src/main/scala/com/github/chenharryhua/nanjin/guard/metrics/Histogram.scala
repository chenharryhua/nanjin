package com.github.chenharryhua.nanjin.guard.metrics

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.applicative.given
import cats.syntax.functor.given
import cats.{Applicative, Endo}
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.event.{
  Category,
  HistogramKind,
  MetricID,
  MetricLabel,
  MetricName,
  Squants
}
import squants.{Each, Quantity, UnitOfMeasure}

trait Histogram[F[_]]:
  def update(num: Long): F[Unit]

  final def update(num: Int): F[Unit] = update(num.toLong)
end Histogram

object Histogram {
  def noop[F[_]](using F: Applicative[F]): Histogram[F] =
    (_: Long) => F.unit

  private class Impl[F[_]: Sync](
    label: MetricLabel,
    metricRegistry: metrics.MetricRegistry,
    squants: Squants,
    reservoir: Option[metrics.Reservoir],
    name: MetricName)
      extends Histogram[F] {

    private val F = Sync[F]

    private val histogram_name: String =
      MetricID(
        metricLabel = label,
        metricName = name,
        Category.Histogram(kind = HistogramKind.Histogram, squants = squants)
      ).identifier

    private val supplier: metrics.MetricRegistry.MetricSupplier[metrics.Histogram] = () =>
      reservoir match {
        case Some(value) => new metrics.Histogram(value)
        case None => new metrics.Histogram(new metrics.ExponentiallyDecayingReservoir) // default reservoir
      }

    private lazy val histogram: metrics.Histogram =
      metricRegistry.histogram(histogram_name, supplier)

    override def update(num: Long): F[Unit] = F.delay(histogram.update(num))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(histogram_name)).void

  }

  final class Builder private[Histogram] (
    isEnabled: Boolean,
    squants: Squants,
    reservoir: Option[metrics.Reservoir])
      extends EnableConfig[Builder] {

    def withReservoir(reservoir: metrics.Reservoir): Builder =
      new Builder(isEnabled, squants, Some(reservoir))

    def withUnit[A <: Quantity[A]](um: UnitOfMeasure[A]): Builder =
      new Builder(isEnabled, Squants(um), reservoir)

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, squants, reservoir)

    private[Histogram] def build[F[_]](
      label: MetricLabel,
      name: String,
      metricRegistry: metrics.MetricRegistry)(using F: Sync[F]): Resource[F, Histogram[F]] = {
      val histogram: Resource[F, Histogram[F]] =
        Resource.make(MetricName(name).map { metricName =>
          new Impl[F](
            label = label,
            metricRegistry = metricRegistry,
            squants = squants,
            reservoir = reservoir,
            name = metricName)
        })(_.unregister)

      if isEnabled then histogram else noop[F].pure
    }
  }

  private[metrics] def apply[F[_]: Sync](
    mr: metrics.MetricRegistry,
    label: MetricLabel,
    name: String,
    f: Endo[Builder]): Resource[F, Histogram[F]] =
    f(new Builder(isEnabled = true, squants = Squants(Each), reservoir = None))
      .build[F](label, name, mr)
}
