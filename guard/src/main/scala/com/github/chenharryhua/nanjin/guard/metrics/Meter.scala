package com.github.chenharryhua.nanjin.guard.metrics

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.applicative.given
import cats.syntax.functor.given
import cats.{Applicative, Endo}
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.event.{
  Category,
  MeterKind,
  MetricID,
  MetricLabel,
  MetricName,
  Squants
}
import squants.{Each, Quantity, UnitOfMeasure}
trait Meter[F[_]]:
  def mark(num: Long): F[Unit]

  final def mark(num: Int): F[Unit] = mark(num.toLong)
end Meter

object Meter {
  def noop[F[_]](using F: Applicative[F]): Meter[F] =
    (_: Long) => F.unit

  private class Impl[F[_]: Sync](
    label: MetricLabel,
    metricRegistry: metrics.MetricRegistry,
    squants: Squants,
    name: MetricName)
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

  final class Builder private[Meter] (isEnabled: Boolean, squants: Squants) extends EnableConfig[Builder] {

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, squants)

    def withUnit[A <: Quantity[A]](um: UnitOfMeasure[A]): Builder =
      new Builder(isEnabled, Squants(um))

    private[Meter] def build[F[_]](label: MetricLabel, name: String, metricRegistry: metrics.MetricRegistry)(
      using F: Sync[F]): Resource[F, Meter[F]] = {
      val meter: Resource[F, Meter[F]] =
        Resource.make(MetricName(name).map { metricName =>
          new Impl[F](label = label, metricRegistry = metricRegistry, squants = squants, name = metricName)
        })(_.unregister)

      if isEnabled then meter else noop[F].pure
    }
  }

  private[metrics] def apply[F[_]: Sync](
    mr: metrics.MetricRegistry,
    label: MetricLabel,
    name: String,
    f: Endo[Builder]): Resource[F, Meter[F]] =
    f(new Builder(isEnabled = true, squants = Squants(Each)))
      .build[F](label, name, mr)
}
