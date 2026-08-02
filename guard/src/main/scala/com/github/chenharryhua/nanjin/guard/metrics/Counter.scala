package com.github.chenharryhua.nanjin.guard.metrics

import cats.Endo
import cats.effect.implicits.genSpawnOps
import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.applicative.given
import cats.syntax.functor.given
import com.codahale.metrics.{Counter as CodahaleCounter, MetricRegistry}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy}
import com.github.chenharryhua.nanjin.guard.event.{Category, CounterKind, MetricID, MetricLabel, MetricName}

import java.time.ZoneId

trait Counter[F[_]]:
  def inc(num: Long): F[Unit]
  final def inc(num: Int): F[Unit] = inc(num.toLong)
end Counter

trait UnsafeCounter:
  def unsafeInc(num: Long): Unit
  final def unsafeInc(num: Int): Unit = unsafeInc(num.toLong)

object Counter {

  private class Impl[F[_]](
    label: MetricLabel,
    metricRegistry: MetricRegistry,
    isRisk: Boolean,
    name: MetricName)(using F: Sync[F])
      extends Counter[F] with UnsafeCounter {
    private val metricId: MetricID =
      if isRisk
      then MetricID(label, name, Category.Counter(CounterKind.Risk))
      else MetricID(label, name, Category.Counter(CounterKind.Counter))

    private lazy val (counterName: String, counter: CodahaleCounter) = {
      val id = metricId.identifier
      (id, metricRegistry.counter(id))
    }

    override def unsafeInc(num: Long): Unit = counter.inc(num)
    override def inc(num: Long): F[Unit] = F.delay(counter.inc(num))

    val reset: F[Unit] = F.delay(counter.dec(counter.getCount))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(counterName)).void

  }

  final class Builder private[Counter] (isEnabled: Boolean, isRisk: Boolean, policy: Policy)
      extends EnableConfig[Builder] {

    def asRisk: Builder = new Builder(isEnabled, true, policy)

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
      val counter: Resource[F, Impl[F]] =
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

      lazy val noop: Counter[F] & UnsafeCounter = new Counter[F] with UnsafeCounter {
        override def inc(num: Long): F[Unit] = ().pure
        override def unsafeInc(num: Long): Unit = ()
      }

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
