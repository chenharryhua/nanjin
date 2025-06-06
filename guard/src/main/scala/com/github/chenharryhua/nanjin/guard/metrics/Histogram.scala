package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.effect.kernel.{Resource, Sync}
import cats.implicits.{catsSyntaxTuple2Semigroupal, toFunctorOps}
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.{utils, EnableConfig}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.HistogramKind
import squants.{Quantity, UnitOfMeasure}

trait Histogram[F[_], A] extends KleisliLike[F, A] {
  def update(num: Long): F[Unit]

  final def update(num: Int): F[Unit] = update(num.toLong)

  def run(num: A): F[Unit]
}

object Histogram {
  def noop[F[_], A](implicit F: Applicative[F]): Histogram[F, A] =
    new Histogram[F, A] {
      override def update(num: Long): F[Unit] = F.unit

      override def run(num: A): F[Unit] = F.unit
    }

  private class Impl[F[_]: Sync, A <: Quantity[A]](
    private[this] val label: MetricLabel,
    private[this] val metricRegistry: metrics.MetricRegistry,
    private[this] val unitOfMeasure: UnitOfMeasure[A],
    private[this] val reservoir: Option[metrics.Reservoir],
    private[this] val name: MetricName)
      extends Histogram[F, A] {

    private[this] val F = Sync[F]

    private[this] val histogram_name: String =
      MetricID(
        metricLabel = label,
        metricName = name,
        Category.Histogram(
          kind = HistogramKind.Histogram,
          squants = Squants(unitOfMeasure.symbol, unitOfMeasure(1).dimension.name))
      ).identifier

    private[this] val supplier: metrics.MetricRegistry.MetricSupplier[metrics.Histogram] = () =>
      reservoir match {
        case Some(value) => new metrics.Histogram(value)
        case None => new metrics.Histogram(new metrics.ExponentiallyDecayingReservoir) // default reservoir
      }

    private[this] lazy val histogram: metrics.Histogram =
      metricRegistry.histogram(histogram_name, supplier)

    override def update(num: Long): F[Unit] = F.delay(histogram.update(num))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(histogram_name)).void

    override def run(num: A): F[Unit] = update(num.to(unitOfMeasure).toLong)
  }

  final class Builder[A <: Quantity[A]] private[guard] (
    isEnabled: Boolean,
    unitOfMeasure: UnitOfMeasure[A],
    reservoir: Option[metrics.Reservoir])
      extends EnableConfig[Builder[A]] {

    def withReservoir(reservoir: metrics.Reservoir): Builder[A] =
      new Builder(isEnabled, unitOfMeasure, Some(reservoir))

    override def enable(isEnabled: Boolean): Builder[A] =
      new Builder(isEnabled, unitOfMeasure, reservoir)

    private[guard] def build[F[_]](label: MetricLabel, name: String, metricRegistry: metrics.MetricRegistry)(
      implicit F: Sync[F]): Resource[F, Histogram[F, A]] =
      if (isEnabled) {
        Resource.make((F.monotonic, utils.randomUUID[F]).mapN { case (ts, unique) =>
          new Impl[F, A](
            label = label,
            metricRegistry = metricRegistry,
            unitOfMeasure = unitOfMeasure,
            reservoir = reservoir,
            name = MetricName(name, ts, unique))
        })(_.unregister)
      } else
        Resource.pure(noop[F, A])
  }
}
