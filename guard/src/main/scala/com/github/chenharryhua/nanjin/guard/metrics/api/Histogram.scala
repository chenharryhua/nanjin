package com.github.chenharryhua.nanjin.guard.metrics.api

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
import com.github.chenharryhua.nanjin.guard.metrics.CategoryKind.HistogramKind
import com.github.chenharryhua.nanjin.guard.metrics.{Category, MetricID, MetricLabel, MetricName, Squants}
import squants.{Each, Quantity, UnitOfMeasure}

/** Effectful distribution recorder for observed numeric values. */
trait Histogram[F[_]]:
  /** Record one observed value. */
  def update(num: Long): F[Unit]
  final def update(num: Int): F[Unit] = update(num.toLong)
end Histogram

/** Synchronous histogram handle. */
trait UnsafeHistogram:
  /** Record one observed value immediately. */
  def unsafeUpdate(num: Long): Unit
  final def unsafeUpdate(num: Int): Unit = unsafeUpdate(num.toLong)
end UnsafeHistogram

object Histogram {

  private class Impl[F[_]](
    label: MetricLabel,
    metricRegistry: MetricRegistry,
    squants: Squants,
    reservoir: Option[Reservoir],
    name: MetricName)(using F: Sync[F])
      extends Histogram[F] with UnsafeHistogram {

    private val histogramName: String =
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

    private val histogram: CodahaleHistogram = metricRegistry.histogram(histogramName, supplier)

    override def update(num: Long): F[Unit] = F.delay(histogram.update(num))
    override def unsafeUpdate(num: Long): Unit = histogram.update(num)

    val unregister: F[Unit] = F.delay(metricRegistry.remove(histogramName)).void

  }

  final class Builder private[Histogram] (isEnabled: Boolean, squants: Squants, reservoir: Option[Reservoir])
      extends EnableConfig[Builder] {

    /** Choose the Dropwizard reservoir used to retain observations. */
    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(isEnabled, squants, Some(reservoir))

    /** Attach a squants unit to the reported histogram. */
    def withUnit[A <: Quantity[A]](um: UnitOfMeasure[A]): Builder =
      new Builder(isEnabled, Squants(um), reservoir)

    /** Enable or disable metric registration; disabled histograms become no-ops. */
    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, squants, reservoir)

    private[Histogram] def build[F[_]](label: MetricLabel, name: String, metricRegistry: MetricRegistry)(using
      F: Sync[F]): Resource[F, Histogram[F] & UnsafeHistogram] = {
      def histogram: Resource[F, Histogram[F] & UnsafeHistogram] =
        Resource.make(MetricName(name).map { metricName =>
          new Impl[F](
            label = label,
            metricRegistry = metricRegistry,
            squants = squants,
            reservoir = reservoir,
            name = metricName)
        })(_.unregister)

      def noop: Histogram[F] & UnsafeHistogram = new Histogram[F] with UnsafeHistogram {
        override def update(num: Long): F[Unit] = ().pure
        override def unsafeUpdate(num: Long): Unit = ()
      }

      if isEnabled then histogram else noop.pure
    }
  }

  private[metrics] def apply[F[_]: Sync](
    mr: MetricRegistry,
    label: MetricLabel,
    name: String,
    f: Endo[Builder]): Resource[F, Histogram[F] & UnsafeHistogram] =
    f(new Builder(isEnabled = true, squants = Squants(Each), reservoir = None))
      .build[F](label, name, mr)
}
