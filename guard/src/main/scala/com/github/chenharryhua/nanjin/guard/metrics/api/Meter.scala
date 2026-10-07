package com.github.chenharryhua.nanjin.guard.metrics.api

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.applicative.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.{Applicative, Endo}
import com.codahale.metrics.{Meter as CodahaleMeter, MetricRegistry}
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
import org.typelevel.otel4s.metrics.{Counter as OtelCounter, Meter as OtelMeter}
import squants.{Each, Quantity, UnitOfMeasure}

/** Effectful event-rate meter. */
trait Meter[F[_]]:
  /** Mark `num` events. */
  def mark(num: Long): F[Unit]
  final def mark(num: Int): F[Unit] = mark(num.toLong)
end Meter

object Meter {

  def noop[F[_]: Applicative]: Meter[F] = new Meter[F] {
    override def mark(num: Long): F[Unit] = ().pure
  }

  private class Impl[F[_]] private[Meter] (
    scope: MetricScope,
    metricRegistry: MetricRegistry,
    squants: Squants,
    name: MetricToken,
    userAttributes: List[Attribute[?]],
    otel: OtelCounter[F, Long])(using F: Sync[F])
      extends Meter[F] {

    private val id: MetricId =
      MetricId(
        scope = scope,
        token = name,
        MetricCategory.Meter(kind = MetricKind.Meter.Default, squants = squants)
      )

    private val attributes: List[Attribute[?]] = id.scope.attributesWith(userAttributes)

    private val meter: CodahaleMeter = metricRegistry.meter(id.identifier)

    // Records to Dropwizard and to an otel4s monotonic Counter (no-op when the configured MeterProvider is
    // MeterProvider.noop). nanjin's Meter counts events; the otel SDK derives the rate from the sum.
    override def mark(num: Long): F[Unit] =
      F.delay(meter.mark(num)) >> otel.add(num, attributes*)

    val unregister: F[Unit] = F.delay(metricRegistry.remove(id.identifier)).void

  }

  final class Builder private[Meter] (
    isEnabled: Boolean,
    squants: Squants,
    description: Option[String],
    userAttributes: List[Attribute[?]])
      extends EnableConfig[Builder] {

    /** Enable or disable metric registration; disabled meters become no-ops. */
    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, squants, description, userAttributes)

    /** Attach a human-readable description carried by the OpenTelemetry instrument. */
    def withDescription(description: String): Builder =
      new Builder(isEnabled, squants, Some(description), userAttributes)

    /** Attach a squants unit to the reported meter. */
    def withUnit[A <: Quantity[A]](um: UnitOfMeasure[A]): Builder =
      new Builder(isEnabled, Squants(um), description, userAttributes)

    /** Attach caller-supplied OpenTelemetry point attributes, recorded on every measurement in addition to
      * the framework `nj.*` attributes. They are '''static''': fixed for the life of the instrument and
      * applied to every `mark`, not per measurement — for a dimension that varies per event, create a
      * separate instrument. Keep them low-cardinality (known at construction), since each distinct attribute
      * set is a separate OpenTelemetry series. Keys starting with `nj.` are ignored so the framework
      * dimensions cannot be overridden. Attributes affect only the OpenTelemetry export, not the Dropwizard
      * snapshot. Repeated calls accumulate.
      */
    def withAttributes(attributes: Attribute[?]*): Builder =
      new Builder(isEnabled, squants, description, userAttributes ::: attributes.toList)

    private[Meter] def build[F[_]](
      scope: MetricScope,
      name: String,
      metricRegistry: MetricRegistry,
      otelMeter: OtelMeter[F])(using F: Sync[F]): Resource[F, Meter[F]] = {
      def meter: Resource[F, Meter[F]] =
        for {
          otel <- Resource.eval {
            val builder = otelMeter.counter[Long](name).withUnit(squants.unitSymbol)
            description.fold(builder)(builder.withDescription).create
          }
          m <- Resource.make(MetricToken(name).map { metricName =>
            new Impl[F](
              scope = scope,
              metricRegistry = metricRegistry,
              squants = squants,
              name = metricName,
              userAttributes = userAttributes,
              otel = otel)
          })(_.unregister)
        } yield m

      if isEnabled then meter else noop.pure
    }
  }

  private[metrics] def apply[F[_]: Sync](
    mr: MetricRegistry,
    scope: MetricScope,
    name: String,
    otelMeter: OtelMeter[F],
    f: Endo[Builder]): Resource[F, Meter[F]] =
    f(new Builder(isEnabled = true, squants = Squants(Each), description = None, userAttributes = Nil))
      .build[F](scope, name, mr, otelMeter)
}
