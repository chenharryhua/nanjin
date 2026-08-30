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
  MetricID,
  MetricKind,
  MetricLabel,
  MetricName,
  Squants
}
import org.typelevel.otel4s.metrics.{Counter as OtelCounter, MeterProvider}
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

  private class Impl[F[_]](
    label: MetricLabel,
    metricRegistry: MetricRegistry,
    squants: Squants,
    name: MetricName,
    otel: OtelCounter[F, Long])(using F: Sync[F])
      extends Meter[F] {

    private val id: MetricID =
      MetricID(
        metricLabel = label,
        metricName = name,
        MetricCategory.Meter(kind = MetricKind.Meter.Default, squants = squants)
      )

    private val meter: CodahaleMeter = metricRegistry.meter(id.identifier)

    // Records to Dropwizard and to an otel4s monotonic Counter (no-op when the configured MeterProvider is
    // MeterProvider.noop). nanjin's Meter counts events; the otel SDK derives the rate from the sum.
    override def mark(num: Long): F[Unit] =
      F.delay(meter.mark(num)) >> otel.add(num, id.attributes)

    val unregister: F[Unit] = F.delay(metricRegistry.remove(id.identifier)).void

  }

  final class Builder private[Meter] (isEnabled: Boolean, squants: Squants, description: Option[String])
      extends EnableConfig[Builder] {

    /** Enable or disable metric registration; disabled meters become no-ops. */
    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, squants, description)

    /** Attach a human-readable description carried by the OpenTelemetry instrument. */
    def withDescription(description: String): Builder =
      new Builder(isEnabled, squants, Some(description))

    /** Attach a squants unit to the reported meter. */
    def withUnit[A <: Quantity[A]](um: UnitOfMeasure[A]): Builder =
      new Builder(isEnabled, Squants(um), description)

    private[Meter] def build[F[_]](
      label: MetricLabel,
      name: String,
      metricRegistry: MetricRegistry,
      meterProvider: MeterProvider[F])(using F: Sync[F]): Resource[F, Meter[F]] = {
      def meter: Resource[F, Meter[F]] =
        for {
          otel <- Resource.eval(meterProvider.get(label.label).flatMap { m =>
            val builder = m.counter[Long](name).withUnit(squants.unitSymbol)
            description.fold(builder)(builder.withDescription).create
          })
          m <- Resource.make(MetricName(name).map { metricName =>
            new Impl[F](
              label = label,
              metricRegistry = metricRegistry,
              squants = squants,
              name = metricName,
              otel = otel)
          })(_.unregister)
        } yield m

      if isEnabled then meter else noop.pure
    }
  }

  private[metrics] def apply[F[_]: Sync](
    mr: MetricRegistry,
    label: MetricLabel,
    name: String,
    meterProvider: MeterProvider[F],
    f: Endo[Builder]): Resource[F, Meter[F]] =
    f(new Builder(isEnabled = true, squants = Squants(Each), description = None))
      .build[F](label, name, mr, meterProvider)
}
