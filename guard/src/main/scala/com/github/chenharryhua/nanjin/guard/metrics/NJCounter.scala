package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.effect.kernel.{Resource, Sync}
import cats.effect.std.UUIDGen
import cats.implicits.{catsSyntaxTuple2Semigroupal, toFunctorOps}
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.CounterKind

sealed trait NJCounter[F[_]] extends KleisliLike[F, Long] {
  def run(a: Long): F[Unit]
  def inc(num: Long): F[Unit]

  final def inc(num: Int): F[Unit] = run(num.toLong)
}

object NJCounter {
  def dummy[F[_]](implicit F: Applicative[F]): NJCounter[F] =
    new NJCounter[F] {
      override def inc(num: Long): F[Unit] = F.unit
      override def run(a: Long): F[Unit]   = F.unit
    }

  private class Impl[F[_]: Sync](
    private[this] val label: MetricLabel,
    private[this] val metricRegistry: MetricRegistry,
    private[this] val isRisk: Boolean,
    private[this] val name: MetricName)
      extends NJCounter[F] {

    private[this] val F = Sync[F]

    private[this] lazy val (counter_name: String, counter: Counter) =
      if (isRisk) {
        val id = MetricID(label, name, Category.Counter(CounterKind.Risk)).identifier
        (id, metricRegistry.counter(id))
      } else {
        val id = MetricID(label, name, Category.Counter(CounterKind.Counter)).identifier
        (id, metricRegistry.counter(id))
      }

    override def run(num: Long): F[Unit] = F.delay(counter.inc(num))
    override def inc(num: Long): F[Unit] = F.delay(counter.inc(num))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(counter_name)).void

  }

  final class Builder private[guard] (isEnabled: Boolean, isRisk: Boolean) extends EnableConfig[Builder] {

    def asRisk: Builder = new Builder(isEnabled, true)

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, isRisk)

    private[guard] def build[F[_]](label: MetricLabel, name: String, metricRegistry: MetricRegistry)(implicit
      F: Sync[F]): Resource[F, NJCounter[F]] =
      if (isEnabled) {
        Resource.make((F.monotonic, UUIDGen[F].randomUUID).mapN { case (ts, unique) =>
          new Impl[F](label, metricRegistry, isRisk, MetricName(name, ts, unique))
        })(_.unregister)
      } else
        Resource.pure(dummy[F])
  }
}
