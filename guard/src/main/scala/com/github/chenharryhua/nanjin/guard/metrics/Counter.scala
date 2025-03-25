package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.effect.kernel.{Resource, Sync}
import cats.implicits.{catsSyntaxTuple2Semigroupal, toFunctorOps}
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.{utils, EnableConfig}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.CounterKind

trait Counter[F[_]] extends KleisliLike[F, Long] {
  def inc(num: Long): F[Unit]

  final def inc(num: Int): F[Unit] = run(num.toLong)

  final override def run(num: Long): F[Unit] = inc(num)
}

object Counter {
  def noop[F[_]](implicit F: Applicative[F]): Counter[F] =
    (_: Long) => F.unit

  private class Impl[F[_]: Sync](
    private[this] val label: MetricLabel,
    private[this] val metricRegistry: metrics.MetricRegistry,
    private[this] val isRisk: Boolean,
    private[this] val name: MetricName)
      extends Counter[F] {

    private[this] val F = Sync[F]

    private[this] lazy val (counter_name: String, counter: metrics.Counter) =
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

  val initial: Builder = new Builder(isEnabled = true, isRisk = false)

  final class Builder private[guard] (isEnabled: Boolean, isRisk: Boolean) extends EnableConfig[Builder] {

    def asRisk: Builder = new Builder(isEnabled, true)

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, isRisk)

    private[guard] def build[F[_]](label: MetricLabel, name: String, metricRegistry: metrics.MetricRegistry)(
      implicit F: Sync[F]): Resource[F, Counter[F]] =
      if (isEnabled) {
        Resource.make((F.monotonic, utils.randomUUID[F]).mapN { case (ts, unique) =>
          new Impl[F](label, metricRegistry, isRisk, MetricName(name, ts, unique))
        })(_.unregister)
      } else
        Resource.pure(noop[F])
  }
}
