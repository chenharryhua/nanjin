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
  MetricId,
  MetricKind,
  MetricScope,
  MetricToken,
  Squants
}
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.{BucketBoundaries, Histogram as OtelHistogram, Meter as OtelMeter}
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

  private class Impl[F[_]] private[Histogram] (
    scope: MetricScope,
    metricRegistry: MetricRegistry,
    squants: Squants,
    reservoir: Option[Reservoir],
    name: MetricToken,
    userAttributes: List[Attribute[String]],
    otel: OtelHistogram[F, Long])(using F: Sync[F])
      extends Histogram[F] {

    private val id: MetricId =
      MetricId(
        scope = scope,
        token = name,
        MetricCategory.Histogram(kind = MetricKind.Histogram.Default, squants = squants)
      )

    private val supplier: MetricRegistry.MetricSupplier[CodahaleHistogram] = () =>
      reservoir match {
        case Some(value) => new CodahaleHistogram(value)
        case None        => new CodahaleHistogram(new ExponentiallyDecayingReservoir) // default reservoir
      }

    private val histogram: CodahaleHistogram = metricRegistry.histogram(id.identifier, supplier)

    private val attributes: List[Attribute[String]] = id.scope.attributesWith(userAttributes)

    // Records to Dropwizard and to an otel4s Histogram (no-op when the configured MeterProvider is
    // MeterProvider.noop). otel4s histograms record Double, so the Long value is widened.
    override def update(num: Long): F[Unit] =
      F.delay(histogram.update(num)) >> otel.record(num, attributes*)

    val unregister: F[Unit] = F.delay(metricRegistry.remove(id.identifier)).void

  }

  final class Builder private[Histogram] (
    isEnabled: Boolean,
    squants: Squants,
    reservoir: Option[Reservoir],
    description: Option[String],
    boundaries: Option[BucketBoundaries],
    userAttributes: List[Attribute[String]])
      extends EnableConfig[Builder] {

    /** Choose the Dropwizard reservoir used to retain observations. */
    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(isEnabled, squants, Some(reservoir), description, boundaries, userAttributes)

    /** Attach a human-readable description carried by the OpenTelemetry instrument. */
    def withDescription(description: String): Builder =
      new Builder(isEnabled, squants, reservoir, Some(description), boundaries, userAttributes)

    /** Attach a squants unit to the reported histogram. */
    def withUnit[A <: Quantity[A]](um: UnitOfMeasure[A]): Builder =
      new Builder(isEnabled, Squants(um), reservoir, description, boundaries, userAttributes)

    /** Enable or disable metric registration; disabled histograms become no-ops. */
    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, squants, reservoir, description, boundaries, userAttributes)

    /** Attach caller-supplied OpenTelemetry point attributes, recorded on every `update` in addition to the
      * framework `nj.*` attributes. They are '''static''': fixed for the life of the instrument, not per
      * measurement — for a dimension that varies per event, create a separate instrument. Keep them
      * low-cardinality, since each distinct attribute set is a separate OpenTelemetry series. On a key
      * conflict the user attribute wins (over the framework `nj.*` dimensions, and the first wins over a
      * later duplicate key). Attributes affect only the OpenTelemetry export, not the Dropwizard snapshot.
      * Repeated calls accumulate.
      */
    def withAttributes(attributes: (String, String)*): Builder =
      new Builder(
        isEnabled,
        squants,
        reservoir,
        description,
        boundaries,
        userAttributes ::: attributes.toList.map(Attribute(_, _)))

    private[Histogram] def build[F[_]](
      scope: MetricScope,
      name: String,
      metricRegistry: MetricRegistry,
      otelMeter: OtelMeter[F])(using F: Sync[F]): Resource[F, Histogram[F]] = {
      def histogram: Resource[F, Histogram[F]] =
        for {
          otel <- Resource.eval(
            ContT.pure(otelMeter.histogram[Long](name).withUnit(squants.unitSymbol))
              .map(b => boundaries.fold(b)(b.withExplicitBucketBoundaries))
              .map(b => description.fold(b)(b.withDescription))
              .run(_.create))
          h <- Resource.make(MetricToken(name).map { metricName =>
            new Impl[F](
              scope = scope,
              metricRegistry = metricRegistry,
              squants = squants,
              reservoir = reservoir,
              name = metricName,
              userAttributes = userAttributes,
              otel = otel)
          })(_.unregister)
        } yield h

      if isEnabled then histogram else noop.pure
    }
  }

  private[metrics] def apply[F[_]: Sync](
    mr: MetricRegistry,
    scope: MetricScope,
    name: String,
    otelMeter: OtelMeter[F],
    f: Endo[Builder]): Resource[F, Histogram[F]] =
    f(
      new Builder(
        isEnabled = true,
        squants = Squants(Each),
        reservoir = None,
        description = None,
        boundaries = None,
        userAttributes = Nil))
      .build[F](scope, name, mr, otelMeter)
}
