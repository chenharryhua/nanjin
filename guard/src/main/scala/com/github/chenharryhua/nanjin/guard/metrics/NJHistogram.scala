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
  def unsafeUpdate(num: Long): Unit
  def update(num: Long): F[Unit]
  final def kleisli[A](f: A => Long): Kleisli[F, A, Unit] =
    Kleisli(update).local(f)
}

private class NJHistogramImpl[F[_]: Sync](
  private[this] val token: Unique.Token,
  private[this] val name: MetricName,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val unit: MeasurementUnit,
  private[this] val reservoir: Option[Reservoir],
  private[this] val tag: MetricTag)
    extends NJHistogram[F] {

  private[this] val F = Sync[F]

  private[this] val histogram_name: String =
    MetricID(name, Category.Histogram(HistogramKind.Histogram, tag, unit), token).identifier

  private[this] val supplier: MetricRegistry.MetricSupplier[Histogram] = () =>
    reservoir match {
      case Some(value) => new Histogram(value)
      case None        => new Histogram(new ExponentiallyDecayingReservoir) // default reservoir
    }

  private[this] lazy val histogram: Histogram =
    metricRegistry.histogram(histogram_name, supplier)

  override def unsafeUpdate(num: Long): Unit = histogram.update(num)
  override def update(num: Long): F[Unit]    = F.delay(histogram.update(num))

  val unregister: F[Unit] = F.delay(metricRegistry.remove(histogram_name)).void
}

object NJHistogram {
  def dummy[F[_]](implicit F: Applicative[F]): NJHistogram[F] =
    new NJHistogram[F] {
      override def unsafeUpdate(num: Long): Unit = ()
      override def update(num: Long): F[Unit]    = F.unit
    }

  final class Builder private[guard] (
    isEnabled: Boolean,
    metricName: MetricName,
    unit: MeasurementUnit,
    reservoir: Option[Reservoir])
      extends EnableConfig[Builder] {

    def withUnit(f: NJUnits.type => MeasurementUnit): Builder =
      new Builder(isEnabled, metricName, f(NJUnits), reservoir)

    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(isEnabled, metricName, unit, Some(reservoir))

    override def enable(value: Boolean): Builder =
      new Builder(value, metricName, unit, reservoir)

    private[guard] def build[F[_]](tag: MetricTag, metricRegistry: MetricRegistry)(implicit
      F: Sync[F]): Resource[F, NJHistogram[F]] =
      if (isEnabled) {
        Resource.make(
          F.unique.map(token =>
            new NJHistogramImpl[F](
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
