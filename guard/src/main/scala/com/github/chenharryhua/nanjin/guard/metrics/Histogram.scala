package com.github.chenharryhua.nanjin.guard.metrics

import cats.Endo
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.applicative.given
import cats.syntax.functor.given
import com.codahale.metrics.{
  ExponentiallyDecayingReservoir,
  Histogram as CodahaleHistogram,
  MetricRegistry,
  Reservoir
}
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

  private class Impl[F[_]](
    label: MetricLabel,
    metricRegistry: MetricRegistry,
    squants: Squants,
    reservoir: Option[Reservoir],
    name: MetricName)(using F: Sync[F])
      extends Histogram[F] {

    private val histogram_name: String =
      MetricID(
        metricLabel = label,
        metricName = name,
        Category.Histogram(kind = HistogramKind.Histogram, squants = squants)
      ).identifier

    private val supplier: MetricRegistry.MetricSupplier[CodahaleHistogram] = () =>
      reservoir match {
        case Some(value) => new CodahaleHistogram(value)
        case None        => new CodahaleHistogram(new ExponentiallyDecayingReservoir) // default reservoir
      }

    private lazy val histogram: CodahaleHistogram =
      metricRegistry.histogram(histogram_name, supplier)

    override def update(num: Long): F[Unit] = F.delay(histogram.update(num))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(histogram_name)).void

  }

  final class Builder private[Histogram] (isEnabled: Boolean, squants: Squants, reservoir: Option[Reservoir])
      extends EnableConfig[Builder] {

    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(isEnabled, squants, Some(reservoir))

    def withUnit[A <: Quantity[A]](um: UnitOfMeasure[A]): Builder =
      new Builder(isEnabled, Squants(um), reservoir)

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, squants, reservoir)

    private[Histogram] def build[F[_]](label: MetricLabel, name: String, metricRegistry: MetricRegistry)(using
      F: Sync[F]): Resource[F, Histogram[F]] = {
      val histogram: Resource[F, Histogram[F]] =
        Resource.make(MetricName(name).map { metricName =>
          new Impl[F](
            label = label,
            metricRegistry = metricRegistry,
            squants = squants,
            reservoir = reservoir,
            name = metricName)
        })(_.unregister)

      val noop: Histogram[F] = (_: Long) => F.unit

      if isEnabled then histogram else noop.pure
    }
  }

  private[metrics] def apply[F[_]: Sync](
    mr: MetricRegistry,
    label: MetricLabel,
    name: String,
    f: Endo[Builder]): Resource[F, Histogram[F]] =
    f(new Builder(isEnabled = true, squants = Squants(Each), reservoir = None))
      .build[F](label, name, mr)
}
