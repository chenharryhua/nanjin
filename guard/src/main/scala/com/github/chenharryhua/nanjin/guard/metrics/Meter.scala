package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.effect.kernel.{Resource, Sync}
import cats.implicits.toFunctorOps
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.MeterKind
import squants.{Quantity, UnitOfMeasure}

trait Meter[F[_], A] extends KleisliLike[F, A] {
  def mark(num: Long): F[Unit]

  final def mark(num: Int): F[Unit] = mark(num.toLong)

  override def run(num: A): F[Unit]
}

object Meter {
  def noop[F[_], A](implicit F: Applicative[F]): Meter[F, A] = new Meter[F, A] {
    override def mark(num: Long): F[Unit] = F.unit
    override def run(num: A): F[Unit] = F.unit
  }

  private class Impl[F[_]: Sync, A <: Quantity[A]](
    private[this] val label: MetricLabel,
    private[this] val metricRegistry: metrics.MetricRegistry,
    private[this] val unitOfMeasure: UnitOfMeasure[A],
    private[this] val name: MetricName)
      extends Meter[F, A] {

    private[this] val F = Sync[F]

    private[this] val meter_name: String =
      MetricID(
        metricLabel = label,
        metricName = name,
        Category.Meter(
          kind = MeterKind.Meter,
          squants = Squants(unitOfMeasure.symbol, unitOfMeasure(1).dimension.name))
      ).identifier

    private[this] lazy val meter: metrics.Meter = metricRegistry.meter(meter_name)

    override def mark(num: Long): F[Unit] = F.delay(meter.mark(num))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(meter_name)).void

    override def run(num: A): F[Unit] = mark(num.to(unitOfMeasure).toLong)
  }

  final class Builder[A <: Quantity[A]] private[guard] (isEnabled: Boolean, unitOfMeasure: UnitOfMeasure[A])
      extends EnableConfig[Builder[A]] {

    override def enable(isEnabled: Boolean): Builder[A] =
      new Builder(isEnabled, unitOfMeasure)

    private[guard] def build[F[_]](label: MetricLabel, name: String, metricRegistry: metrics.MetricRegistry)(
      implicit F: Sync[F]): Resource[F, Meter[F, A]] = {
      val meter: Resource[F, Meter[F, A]] =
        Resource.make(MetricName(name).map { metricName =>
          new Impl[F, A](
            label = label,
            metricRegistry = metricRegistry,
            unitOfMeasure = unitOfMeasure,
            name = metricName)
        })(_.unregister)

      fold_create_noop(isEnabled)(meter, Resource.pure(noop[F, A]))
    }
  }
}
