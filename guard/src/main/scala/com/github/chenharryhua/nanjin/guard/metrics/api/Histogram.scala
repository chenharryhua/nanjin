package com.github.chenharryhua.nanjin.guard.metrics.api

import cats.data.ContT
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.applicative.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.{Applicative, Endo}
import com.codahale.metrics.{
  ExponentiallyDecayingReservoir,
  Histogram as CodahaleHistogram,
  MetricRegistry,
  Reservoir
}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.metrics.{
  MetricCategory,
  MetricID,
  MetricKind,
  MetricLabel,
  MetricName,
  Squants
}
import org.typelevel.otel4s.metrics.{BucketBoundaries, Histogram as OtelHistogram, MeterProvider}
import squants.{Each, Quantity, UnitOfMeasure}

/** Effectful distribution recorder for observed numeric values. */
trait Histogram[F[_]]:
  /** Record one observed value. */
  def update(num: Long): F[Unit]
  final def update(num: Int): F[Unit] = update(num.toLong)
end Histogram

object Histogram {

  def noop[F[_]: Applicative]: Histogram[F] = new Histogram[F] {
    override def update(num: Long): F[Unit] = ().pure
  }

  private class Impl[F[_]](
    label: MetricLabel,
    metricRegistry: MetricRegistry,
    squants: Squants,
    reservoir: Option[Reservoir],
    name: MetricName,
    otel: OtelHistogram[F, Long])(using F: Sync[F])
      extends Histogram[F] {

    private val id: MetricID =
      MetricID(
        metricLabel = label,
        metricName = name,
        MetricCategory.Histogram(kind = MetricKind.Histogram.Default, squants = squants)
      )

    private val supplier: MetricRegistry.MetricSupplier[CodahaleHistogram] = () =>
      reservoir match {
        case Some(value) => new CodahaleHistogram(value)
        case None        => new CodahaleHistogram(new ExponentiallyDecayingReservoir) // default reservoir
      }

    private val histogram: CodahaleHistogram = metricRegistry.histogram(id.identifier, supplier)

    // Records to Dropwizard and to an otel4s Histogram (no-op when the configured MeterProvider is
    // MeterProvider.noop). otel4s histograms record Double, so the Long value is widened.
    override def update(num: Long): F[Unit] =
      F.delay(histogram.update(num)) >> otel.record(num, id.attributes)

    val unregister: F[Unit] = F.delay(metricRegistry.remove(id.identifier)).void

  }

  final class Builder private[Histogram] (
    isEnabled: Boolean,
    squants: Squants,
    reservoir: Option[Reservoir],
    description: Option[String],
    boundaries: Option[BucketBoundaries])
      extends EnableConfig[Builder] {

    /** Choose the Dropwizard reservoir used to retain observations. */
    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(isEnabled, squants, Some(reservoir), description, boundaries)

    /** Attach a human-readable description carried by the OpenTelemetry instrument. */
    def withDescription(description: String): Builder =
      new Builder(isEnabled, squants, reservoir, Some(description), boundaries)

    /** Attach a squants unit to the reported histogram. */
    def withUnit[A <: Quantity[A]](um: UnitOfMeasure[A]): Builder =
      new Builder(isEnabled, Squants(um), reservoir, description, boundaries)

    /** Enable or disable metric registration; disabled histograms become no-ops. */
    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, squants, reservoir, description, boundaries)

    private[Histogram] def build[F[_]](
      label: MetricLabel,
      name: String,
      metricRegistry: MetricRegistry,
      meterProvider: MeterProvider[F])(using F: Sync[F]): Resource[F, Histogram[F]] = {
      def histogram: Resource[F, Histogram[F]] =
        for {
          otel <- Resource.eval(meterProvider.get(label.label).flatMap { m =>
            ContT.pure(m.histogram[Long](name).withUnit(squants.unitSymbol))
              .map(b => boundaries.fold(b)(b.withExplicitBucketBoundaries))
              .map(b => description.fold(b)(b.withDescription))
              .run(_.create)
          })
          h <- Resource.make(MetricName(name).map { metricName =>
            new Impl[F](
              label = label,
              metricRegistry = metricRegistry,
              squants = squants,
              reservoir = reservoir,
              name = metricName,
              otel = otel)
          })(_.unregister)
        } yield h

      if isEnabled then histogram else noop.pure
    }
  }

  private[metrics] def apply[F[_]: Sync](
    mr: MetricRegistry,
    label: MetricLabel,
    name: String,
    meterProvider: MeterProvider[F],
    f: Endo[Builder]): Resource[F, Histogram[F]] =
    f(
      new Builder(
        isEnabled = true,
        squants = Squants(Each),
        reservoir = None,
        description = None,
        boundaries = None))
      .build[F](label, name, mr, meterProvider)
}
