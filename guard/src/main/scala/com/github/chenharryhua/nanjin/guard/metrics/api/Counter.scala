package com.github.chenharryhua.nanjin.guard.metrics.api

import cats.effect.implicits.genSpawnOps
import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.applicative.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.{Applicative, Endo}
import com.codahale.metrics.{Counter as CodahaleCounter, MetricRegistry}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy}
import com.github.chenharryhua.nanjin.guard.metrics.{
  MetricCategory,
  MetricId,
  MetricKind,
  MetricScope,
  MetricToken
}
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.{MeterProvider, UpDownCounter}
import squants.Each

import java.time.ZoneId

/** Effectful monotonically adjustable count. Acquire it from `MetricsHub.counter`. */
trait Counter[F[_]]:
  /** Add `num` to the counter inside `F`. */
  def inc(num: Long): F[Unit]
  final def inc(num: Int): F[Unit] = inc(num.toLong)
end Counter

object Counter {

  private class Impl[F[_]](
    scope: MetricScope,
    metricRegistry: MetricRegistry,
    isRisk: Boolean,
    name: MetricToken,
    upDown: UpDownCounter[F, Long])(using F: Sync[F])
      extends Counter[F] {
    private val id: MetricId =
      if isRisk
      then MetricId(scope, name, MetricCategory.Counter(MetricKind.Counter.Risk))
      else MetricId(scope, name, MetricCategory.Counter(MetricKind.Counter.Default))

    private val counter: CodahaleCounter = metricRegistry.counter(id.identifier)

    // nanjin's conceptual grouping attributes (nj.domain / nj.service / nj.task). Note: risk and normal
    // counters are NOT separated by a point attribute here, so under the same metric name they aggregate
    // into a single OpenTelemetry series. Distinguish them via the metric name if separate series are needed.
    private val attributes: List[Attribute[String]] = id.attributes

    // Records to Dropwizard and to the otel4s UpDownCounter (a no-op when the configured MeterProvider is
    // MeterProvider.noop). nanjin's Counter maps to UpDownCounter because inc accepts negative deltas.
    override def inc(num: Long): F[Unit] =
      F.delay(counter.inc(num)) >> upDown.add(num, attributes)

    // Dropwizard-only, by design. The policy reset keeps the Dropwizard count cumulative only within the
    // current reporting window. The otel4s UpDownCounter is intentionally NOT reset: OpenTelemetry
    // instruments are cumulative and the backend owns windowing via its export temporality (delta vs
    // cumulative). Zeroing it here would create artificial sawtooth resets that break downstream
    // rate()/increase() calculations. The two backends therefore report different absolute values across a
    // reset tick, which is correct: they answer different questions.
    val reset: F[Unit] = F.delay(counter.dec(counter.getCount))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(id.identifier)).void

  }

  def noop[F[_]: Applicative]: Counter[F] = new Counter[F] {
    override def inc(num: Long): F[Unit] = ().pure
  }

  final class Builder private[Counter] (
    isEnabled: Boolean,
    isRisk: Boolean,
    policy: Policy,
    description: Option[String])
      extends EnableConfig[Builder] {

    /** Classify the counter as a risk counter in reported metrics. */
    def asRisk: Builder = new Builder(isEnabled, true, policy, description)

    /** Attach a human-readable description carried by the OpenTelemetry instrument. */
    def withDescription(description: String): Builder =
      new Builder(isEnabled, isRisk, policy, Some(description))

    /** Enable or disable metric registration; disabled counters become no-ops. */
    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, isRisk, policy, description)

    /** Reset the counter to zero whenever the supplied policy emits a tick.
      */
    def withPolicy(f: Policy.type => Policy): Builder =
      new Builder(isEnabled, isRisk, f(Policy), description)

    private[Counter] def build[F[_]: Async](
      scope: MetricScope,
      name: String,
      metricRegistry: MetricRegistry,
      zoneId: ZoneId,
      meterProvider: MeterProvider[F]): Resource[F, Counter[F]] = {
      def counter: Resource[F, Impl[F]] =
        for {
          upDown <- Resource.eval(meterProvider.get(scope.label).flatMap { m =>
            val builder = m.upDownCounter[Long](name).withUnit(Each.symbol)
            description.fold(builder)(builder.withDescription).create
          })
          counter <- Resource.make(
            MetricToken(name)
              .map { metricName =>
                new Impl[F](scope, metricRegistry, isRisk, metricName, upDown)
              })(_.unregister)
          // Keep the counter cumulative only within the current policy window.
          _ <- tickStream.tickScheduled[F](zoneId, _.fresh(policy))
            .evalMap(_ => counter.reset)
            .compile
            .drain
            .background
        } yield counter

      if isEnabled then counter else noop.pure
    }
  }

  private[metrics] def apply[F[_]: Async](
    mr: MetricRegistry,
    scope: MetricScope,
    name: String,
    zoneId: ZoneId,
    meterProvider: MeterProvider[F],
    f: Endo[Builder]): Resource[F, Counter[F]] =
    f(new Builder(isEnabled = true, isRisk = false, policy = Policy.empty, description = None))
      .build[F](scope, name, mr, zoneId, meterProvider)
}
