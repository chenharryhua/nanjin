package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.functor.toFunctorOps
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.event.CategoryKind.MeterKind
import com.github.chenharryhua.nanjin.guard.event.{Category, MetricID, MetricLabel, MetricName, Squants}
import squants.{Quantity, UnitOfMeasure}
trait Meter[F[_]]:
  def mark(num: Long): F[Unit]

  final def mark(num: Int): F[Unit] =
    mark(num.toLong)
end Meter

object Meter {
  def noop[F[_]](using F: Applicative[F]): Meter[F] = new Meter[F] {
    override def mark(num: Long): F[Unit] = F.unit
  }

  private class Impl[F[_]: Sync](
    private val label: MetricLabel,
    private val metricRegistry: metrics.MetricRegistry,
    private val squants: Squants,
    private val name: MetricName)
      extends Meter[F] {

    private val F = Sync[F]

    private val meter_name: String =
      MetricID(
        metricLabel = label,
        metricName = name,
        Category.Meter(kind = MeterKind.Meter, squants = squants)
      ).identifier

    private lazy val meter: metrics.Meter = metricRegistry.meter(meter_name)

    override def mark(num: Long): F[Unit] = F.delay(meter.mark(num))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(meter_name)).void

  }

  final class Builder private[guard] (isEnabled: Boolean, squants: Squants) extends EnableConfig[Builder] {

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, squants)

    def withUnit[A <: Quantity[A]](um: UnitOfMeasure[A]): Builder =
      new Builder(isEnabled, Squants(um))

    private[guard] def build[F[_]](label: MetricLabel, name: String, metricRegistry: metrics.MetricRegistry)(
      using F: Sync[F]): Resource[F, Meter[F]] = {
      val meter: Resource[F, Meter[F]] =
        Resource.make(MetricName(name).map { metricName =>
          new Impl[F](label = label, metricRegistry = metricRegistry, squants = squants, name = metricName)
        })(_.unregister)

      fold_create_noop(isEnabled)(meter, Resource.pure(noop[F]))
    }
  }
}
