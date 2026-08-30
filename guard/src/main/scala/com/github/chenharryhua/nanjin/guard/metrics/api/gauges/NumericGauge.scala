package com.github.chenharryhua.nanjin.guard.metrics.api.gauges

import cats.Endo
import cats.data.ContT
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.codahale.metrics.{Gauge as CodahaleGauge, MetricRegistry}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.metrics.{
  MetricCategory,
  MetricID,
  MetricKind,
  MetricLabel,
  MetricName,
  Squants
}
import io.circe.Json
import org.typelevel.otel4s.metrics.{MeterProvider, ObservableGauge}
import squants.{Each, Quantity, UnitOfMeasure}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.control.NonFatal

/** A pull-based `Long` gauge that records to both Dropwizard and, when a real
  * `org.typelevel.otel4s.metrics.MeterProvider` is configured, an otel4s `ObservableGauge`.
  *
  * This is deliberately distinct from `Gauge`: `Gauge` stores an arbitrary `io.circe.Encoder` value as JSON
  * and is Dropwizard-only, because an arbitrary encoded value cannot satisfy otel4s's numeric
  * `MeasurementValue`. `NumericGauge` fixes the value to `Long` so it maps to an OpenTelemetry gauge point,
  * consistent with the other instruments (counter/meter/histogram/timer all speak `Long`). Choose a unit
  * whose scale makes the integer natural: report `35` with a percent unit for 35%, not `0.35`. Finer-grained
  * fractions can use a smaller unit (e.g. per mille) so the reported integer stays meaningful.
  *
  * The value is read on demand: Dropwizard evaluates the effect when the registry is scraped, and the otel4s
  * `ObservableGauge` evaluates it in the collection callback. There is no push/update call site and no
  * refresh policy.
  */
object NumericGauge {

  final class Builder private[NumericGauge] (
    private[NumericGauge] val isEnabled: Boolean,
    private[NumericGauge] val timeout: FiniteDuration,
    private[NumericGauge] val squants: Squants,
    private[NumericGauge] val description: Option[String])
      extends EnableConfig[Builder] {

    /** Enable or disable gauge registration; disabled gauges become no-ops. */
    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, timeout, squants, description)

    /** Bound evaluation time when the Dropwizard registry reads the gauge value. */
    def withTimeout(timeout: FiniteDuration): Builder =
      new Builder(isEnabled, timeout, squants, description)

    /** Attach a squants unit whose symbol is carried by the OpenTelemetry instrument (default `Each`). */
    def withUnit[A <: Quantity[A]](um: UnitOfMeasure[A]): Builder =
      new Builder(isEnabled, timeout, Squants(um), description)

    /** Attach a human-readable description carried by the OpenTelemetry instrument. */
    def withDescription(description: String): Builder =
      new Builder(isEnabled, timeout, squants, Some(description))

    private[NumericGauge] def build[F[_]](
      metricRegistry: MetricRegistry,
      label: MetricLabel,
      name: String,
      dispatcher: Dispatcher[F],
      meterProvider: MeterProvider[F],
      fa: F[Long])(using F: Async[F]): Resource[F, Unit] = {

      // The Dropwizard side stores the value as a JSON number so it flows through the existing snapshot
      // pipeline exactly like a Default gauge. A failed evaluation renders the error as JSON via the shared
      // translateError, matching Gauge, so a broken gauge stays visible in the snapshot rather than vanishing.
      def dropwizard(id: MetricID): Resource[F, Unit] =
        Resource.make(F.delay {
          metricRegistry.gauge(
            id.identifier,
            () =>
              new CodahaleGauge[Json] {
                override def getValue: Json =
                  try dispatcher.unsafeRunTimed(fa.map(Json.fromLong), timeout)
                  catch { case NonFatal(ex) => translateError(ex) }
              }
          )
        })(_ => F.delay(metricRegistry.remove(id.identifier)).void).void

      // The otel4s ObservableGauge reads the same effect in its collection callback. When the configured
      // MeterProvider is MeterProvider.noop this whole resource is a no-op. The optional description is
      // threaded with ContT, mirroring Timer's instrument construction.
      def observable(id: MetricID): Resource[F, Unit] =
        Resource.eval(meterProvider.get(label.label)).flatMap { m =>
          ContT
            .pure[[X] =>> Resource[F, X], Unit, ObservableGauge.Builder[F, Long]](
              m.observableGauge[Long](name).withUnit(squants.unitSymbol))
            .map(b => description.fold(b)(b.withDescription))
            .run(_.createWithCallback(cb => fa.flatMap(a => cb.record(a, id.attributes*))).void)
        }

      def impl: Resource[F, Unit] =
        for {
          id <- Resource.eval(MetricName(name).map(mn =>
            MetricID(label, mn, MetricCategory.Gauge(MetricKind.Gauge.Default, isCached = false))))
          _ <- dropwizard(id)
          _ <- observable(id)
        } yield ()

      if isEnabled then impl else Resource.unit
    }
  }

  private[metrics] def apply[F[_]: Async](
    metricRegistry: MetricRegistry,
    label: MetricLabel,
    name: String,
    dispatcher: Dispatcher[F],
    meterProvider: MeterProvider[F],
    fa: F[Long],
    f: Endo[Builder]): Resource[F, Unit] =
    f(new Builder(isEnabled = true, timeout = 5.seconds, squants = Squants(Each), description = None))
      .build[F](metricRegistry, label, name, dispatcher, meterProvider, fa)
}
