package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.effect.kernel.{Resource, Sync}
import cats.effect.std.UUIDGen
import cats.implicits.{catsSyntaxTuple2Semigroupal, toFunctorOps}
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.HistogramKind
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, NJUnits}

trait NJHistogram[F[_]] extends KleisliLike[F, Long] {
  def update(num: Long): F[Unit]

  final def update(num: Int): F[Unit] = update(num.toLong)

  final override def run(num: Long): F[Unit] = update(num)
}

object NJHistogram {
  def dummy[F[_]](implicit F: Applicative[F]): NJHistogram[F] =
    new NJHistogram[F] {
      override def update(num: Long): F[Unit] = F.unit
    }

  private class Impl[F[_]: Sync](
    private[this] val label: MetricLabel,
    private[this] val metricRegistry: MetricRegistry,
    private[this] val unit: MeasurementUnit,
    private[this] val reservoir: Option[Reservoir],
    private[this] val name: MetricName)
      extends NJHistogram[F] {

    private[this] val F = Sync[F]

    private[this] val histogram_name: String =
      MetricID(label, name, Category.Histogram(HistogramKind.Histogram, unit)).identifier

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

  val initial: Builder = new Builder(isEnabled = true, unit = NJUnits.COUNT, reservoir = None)

  final class Builder private[guard] (isEnabled: Boolean, unit: MeasurementUnit, reservoir: Option[Reservoir])
      extends EnableConfig[Builder] {

    def withUnit(f: NJUnits.type => MeasurementUnit): Builder =
      new Builder(isEnabled, f(NJUnits), reservoir)

    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(isEnabled, unit, Some(reservoir))

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, unit, reservoir)

    private[guard] def build[F[_]](label: MetricLabel, name: String, metricRegistry: MetricRegistry)(implicit
      F: Sync[F]): Resource[F, NJHistogram[F]] =
      if (isEnabled) {
        Resource.make((F.monotonic, UUIDGen[F].randomUUID).mapN { case (ts, unique) =>
          new Impl[F](
            label = label,
            metricRegistry = metricRegistry,
            unit = unit,
            reservoir = reservoir,
            name = MetricName(name, ts, unique))
        })(_.unregister)
      } else
        Resource.pure(dummy[F])
  }
}
