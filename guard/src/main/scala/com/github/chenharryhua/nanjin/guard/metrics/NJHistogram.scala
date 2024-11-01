package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.data.Kleisli
import cats.effect.kernel.{Resource, Sync, Unique}
import cats.implicits.toFunctorOps
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.HistogramKind
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, NJUnits}

sealed trait NJHistogram[F[_]] {
  def update(num: Long): F[Unit]

  final def update(num: Int): F[Unit] = update(num.toLong)
  final def kleisli[A](f: A => Long): Kleisli[F, A, Unit] =
    Kleisli[F, Long, Unit](update).local(f)
  final def kleisli: Kleisli[F, Long, Unit] =
    Kleisli[F, Long, Unit](update)
}

object NJHistogram {
  def dummy[F[_]](implicit F: Applicative[F]): NJHistogram[F] =
    new NJHistogram[F] {
      override def update(num: Long): F[Unit] = F.unit
    }

  private class Impl[F[_]: Sync](
    private[this] val token: Unique.Token,
    private[this] val name: MetricName,
    private[this] val metricRegistry: MetricRegistry,
    private[this] val unit: MeasurementUnit,
    private[this] val reservoir: Option[Reservoir],
    private[this] val tag: MetricTag)
      extends NJHistogram[F] {

    private[this] val F = Sync[F]

    private[this] val histogram_name: String =
      MetricID(name, tag, Category.Histogram(HistogramKind.Histogram, unit), token).identifier

    private[this] val supplier: MetricRegistry.MetricSupplier[Histogram] = () =>
      reservoir match {
        case Some(value) => new Histogram(value)
        case None        => new Histogram(new ExponentiallyDecayingReservoir) // default reservoir
      }

    private[this] lazy val histogram: Histogram =
      metricRegistry.histogram(histogram_name, supplier)

    override def update(num: Long): F[Unit] = F.delay(histogram.update(num))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(histogram_name)).void
  }

  final class Builder private[guard] (isEnabled: Boolean, unit: MeasurementUnit, reservoir: Option[Reservoir])
      extends EnableConfig[Builder] {

    def withUnit(f: NJUnits.type => MeasurementUnit): Builder =
      new Builder(isEnabled, f(NJUnits), reservoir)

    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(isEnabled, unit, Some(reservoir))

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, unit, reservoir)

    private[guard] def build[F[_]](metricName: MetricName, tag: MetricTag, metricRegistry: MetricRegistry)(
      implicit F: Sync[F]): Resource[F, NJHistogram[F]] =
      if (isEnabled) {
        Resource.make(
          F.unique.map(token =>
            new Impl[F](
              token = token,
              name = metricName,
              metricRegistry = metricRegistry,
              unit = unit,
              reservoir = reservoir,
              tag = tag)))(_.unregister)
      } else
        Resource.pure(dummy[F])
  }
}
