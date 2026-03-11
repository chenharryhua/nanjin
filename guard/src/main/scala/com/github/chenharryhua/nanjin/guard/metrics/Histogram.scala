package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.functor.toFunctorOps
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.event.CategoryKind.HistogramKind
import com.github.chenharryhua.nanjin.guard.event.{Category, MetricID, MetricLabel, MetricName, Squants}
import squants.{Quantity, UnitOfMeasure}

trait Histogram[F[_]]:
  def update(num: Long): F[Unit]

  final def update(num: Int): F[Unit] =
    update(num.toLong)
end Histogram

object Histogram {
  def noop[F[_]](implicit F: Applicative[F]): Histogram[F] =
    new Histogram[F] {
      override def update(num: Long): F[Unit] = F.unit
    }

  private class Impl[F[_]: Sync](
    private val label: MetricLabel,
    private val metricRegistry: metrics.MetricRegistry,
    private val squants: Squants,
    private val reservoir: Option[metrics.Reservoir],
    private val name: MetricName)
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

  final class Builder private[guard] (
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

    private[guard] def build[F[_]](label: MetricLabel, name: String, metricRegistry: metrics.MetricRegistry)(
      implicit F: Sync[F]): Resource[F, Histogram[F]] = {
      val histogram: Resource[F, Histogram[F]] =
        Resource.make(MetricName(name).map { metricName =>
          new Impl[F](
            label = label,
            metricRegistry = metricRegistry,
            squants = squants,
            reservoir = reservoir,
            name = metricName)
        })(_.unregister)

      fold_create_noop(isEnabled)(histogram, Resource.pure(noop[F]))
    }
  }
}
