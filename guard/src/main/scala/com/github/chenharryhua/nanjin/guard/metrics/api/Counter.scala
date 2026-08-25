package com.github.chenharryhua.nanjin.guard.metrics.api

import cats.effect.implicits.genSpawnOps
import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.applicative.given
import cats.syntax.functor.given
import cats.{Applicative, Endo}
import com.codahale.metrics.{Counter as CodahaleCounter, MetricRegistry}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy}
import com.github.chenharryhua.nanjin.guard.metrics.{
  MetricCategory,
  MetricID,
  MetricKind,
  MetricLabel,
  MetricName
}

import java.time.ZoneId

/** Effectful monotonically adjustable count. Acquire it from `MetricsHub.counter`. */
trait Counter[F[_]]:
  /** Add `num` to the counter inside `F`. */
  def inc(num: Long): F[Unit]
  final def inc(num: Int): F[Unit] = inc(num.toLong)
end Counter

/** Synchronous counter handle for updates from already-effectful code. */
trait UnsafeCounter:
  /** Add `num` immediately; callers provide their own synchronization and error boundary. */
  def unsafeInc(num: Long): Unit
  final def unsafeInc(num: Int): Unit = unsafeInc(num.toLong)

object Counter {

  private class Impl[F[_]](
    label: MetricLabel,
    metricRegistry: MetricRegistry,
    isRisk: Boolean,
    name: MetricName)(using F: Sync[F])
      extends Counter[F] with UnsafeCounter {
    private val counterName: String =
      if isRisk
      then MetricID(label, name, MetricCategory.Counter(MetricKind.Counter.Risk)).identifier
      else MetricID(label, name, MetricCategory.Counter(MetricKind.Counter.Default)).identifier

    private val counter: CodahaleCounter = metricRegistry.counter(counterName)

    override def unsafeInc(num: Long): Unit = counter.inc(num)
    override def inc(num: Long): F[Unit] = F.delay(counter.inc(num))

    val reset: F[Unit] = F.delay(counter.dec(counter.getCount))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(counterName)).void

  }

  def noop[F[_]: Applicative]: Counter[F] & UnsafeCounter = new Counter[F] with UnsafeCounter {
    override def inc(num: Long): F[Unit] = ().pure
    override def unsafeInc(num: Long): Unit = ()
  }

  final class Builder private[Counter] (isEnabled: Boolean, isRisk: Boolean, policy: Policy)
      extends EnableConfig[Builder] {

    /** Classify the counter as a risk counter in reported metrics. */
    def asRisk: Builder = new Builder(isEnabled, true, policy)

    /** Enable or disable metric registration; disabled counters become no-ops. */
    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, isRisk, policy)

    /** Reset the counter to zero whenever the supplied policy emits a tick.
      */
    def withPolicy(f: Policy.type => Policy): Builder =
      new Builder(isEnabled, isRisk, f(Policy))

    private[Counter] def build[F[_]: Async](
      label: MetricLabel,
      name: String,
      metricRegistry: MetricRegistry,
      zoneId: ZoneId): Resource[F, Counter[F] & UnsafeCounter] = {
      def counter: Resource[F, Impl[F]] =
        for {
          counter <- Resource.make(
            MetricName(name)
              .map { metricName =>
                new Impl[F](label, metricRegistry, isRisk, metricName)
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
    label: MetricLabel,
    name: String,
    zoneId: ZoneId,
    f: Endo[Builder]): Resource[F, Counter[F] & UnsafeCounter] =
    f(new Builder(isEnabled = true, isRisk = false, policy = Policy.empty))
      .build[F](label, name, mr, zoneId)
}
