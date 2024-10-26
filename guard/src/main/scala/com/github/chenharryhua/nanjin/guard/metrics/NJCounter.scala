package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.data.Kleisli
import cats.effect.kernel.{Resource, Sync, Unique}
import cats.implicits.toFunctorOps
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.CounterKind

sealed trait NJCounter[F[_]] {
  def inc(num: Long): F[Unit]
  def dec(num: Long): F[Unit]

  final def kleisli[A](f: A => Long): Kleisli[F, A, Unit] =
    Kleisli(inc).local(f)
}

private class NJCounterImpl[F[_]: Sync](
  private[this] val token: Unique.Token,
  private[this] val name: MetricName,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val isRisk: Boolean,
  private[this] val tag: MetricTag)
    extends NJCounter[F] {

  private[this] val F = Sync[F]

  private[this] lazy val (counter_name: String, counter: Counter) =
    if (isRisk) {
      val id = MetricID(name, tag, Category.Counter(CounterKind.Risk), token).identifier
      (id, metricRegistry.counter(id))
    } else {
      val id = MetricID(name, tag, Category.Counter(CounterKind.Counter), token).identifier
      (id, metricRegistry.counter(id))
    }

  override def inc(num: Long): F[Unit] = F.delay(counter.inc(num))
  override def dec(num: Long): F[Unit] = F.delay(counter.dec(num))

  val unregister: F[Unit] = F.delay(metricRegistry.remove(counter_name)).void
}

object NJCounter {
  def dummy[F[_]](implicit F: Applicative[F]): NJCounter[F] =
    new NJCounter[F] {
      override def inc(num: Long): F[Unit] = F.unit
      override def dec(num: Long): F[Unit] = F.unit
    }

  final class Builder private[guard] (isEnabled: Boolean, isRisk: Boolean) extends EnableConfig[Builder] {

    def asRisk: Builder = new Builder(isEnabled, true)

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, isRisk)

    private[guard] def build[F[_]](metricName: MetricName, tag: MetricTag, metricRegistry: MetricRegistry)(
      implicit F: Sync[F]): Resource[F, NJCounter[F]] =
      if (isEnabled) {
        Resource
          .make(F.unique.map(new NJCounterImpl[F](_, metricName, metricRegistry, isRisk, tag)))(_.unregister)
      } else
        Resource.pure(dummy[F])
  }
}
