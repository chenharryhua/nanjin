package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.functor.toFunctorOps
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.event.CounterKind
import com.github.chenharryhua.nanjin.guard.event.{Category, MetricID, MetricLabel, MetricName}

trait Counter[F[_]]:
  def inc(num: Long): F[Unit]
  extension (c: Counter[F])
    def inc(num: Int): F[Unit] = c.inc(num.toLong)
    def inc(): F[Unit] = c.inc(1)
end Counter

object Counter {
  def noop[F[_]: Applicative]: Counter[F] =
    (_: Long) => ().pure[F]

  private class Impl[F[_]: Sync](
    private val label: MetricLabel,
    private val metricRegistry: metrics.MetricRegistry,
    private val isRisk: Boolean,
    private val name: MetricName)
      extends Counter[F] {

    private val F = Sync[F]

    private lazy val (counter_name: String, counter: metrics.Counter) =
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

  final class Builder private[guard] (isEnabled: Boolean, isRisk: Boolean) extends EnableConfig[Builder] {

    def asRisk: Builder = new Builder(isEnabled, true)

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, isRisk)

    private[guard] def build[F[_]](label: MetricLabel, name: String, metricRegistry: metrics.MetricRegistry)(
      using F: Sync[F]): Resource[F, Counter[F]] = {
      val counter: Resource[F, Counter[F]] =
        Resource.make(MetricName(name).map { metricName =>
          new Impl[F](label, metricRegistry, isRisk, metricName)
        })(_.unregister)

      fold_create_noop(isEnabled)(counter, Resource.pure(noop[F]))
    }
  }
}
