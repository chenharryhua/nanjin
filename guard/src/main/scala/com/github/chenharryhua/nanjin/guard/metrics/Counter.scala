package com.github.chenharryhua.nanjin.guard.metrics

import cats.Endo
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.applicative.given
import cats.syntax.functor.given
import com.codahale.metrics.{Counter as CodahaleCounter, MetricRegistry}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.event.{Category, CounterKind, MetricID, MetricLabel, MetricName}

trait Counter[F[_]]:
  def inc(num: Long): F[Unit]
  final def inc(num: Int): F[Unit] = inc(num.toLong)
end Counter

object Counter {

  private class Impl[F[_]](
    label: MetricLabel,
    metricRegistry: MetricRegistry,
    isRisk: Boolean,
    name: MetricName)(using F: Sync[F])
      extends Counter[F] {

    private lazy val (counter_name: String, counter: CodahaleCounter) =
      if (isRisk) {
        val id = MetricID(label, name, Category.Counter(CounterKind.Risk)).identifier
        (id, metricRegistry.counter(id))
      } else {
        val id = MetricID(label, name, Category.Counter(CounterKind.Counter)).identifier
        (id, metricRegistry.counter(id))
      }

    override def inc(num: Long): F[Unit] = F.delay(counter.inc(num))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(counter_name)).void

  }

  final class Builder private[Counter] (isEnabled: Boolean, isRisk: Boolean) extends EnableConfig[Builder] {

    def asRisk: Builder = new Builder(isEnabled, true)

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, isRisk)

    private[Counter] def build[F[_]](label: MetricLabel, name: String, metricRegistry: MetricRegistry)(using
      F: Sync[F]): Resource[F, Counter[F]] = {
      val counter: Resource[F, Counter[F]] =
        Resource.make(MetricName(name).map { metricName =>
          new Impl[F](label, metricRegistry, isRisk, metricName)
        })(_.unregister)

      val noop: Counter[F] = (_: Long) => ().pure[F]

      if isEnabled then counter else noop.pure
    }
  }

  private[metrics] def apply[F[_]: Sync](
    mr: MetricRegistry,
    label: MetricLabel,
    name: String,
    f: Endo[Builder]): Resource[F, Counter[F]] =
    f(new Builder(isEnabled = true, isRisk = false))
      .build[F](label, name, mr)
}
