package com.github.chenharryhua.nanjin.guard.metrics

import cats.Endo
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.applicative.given
import cats.syntax.functor.given
import com.codahale.metrics.{Meter as CodahaleMeter, MetricRegistry}
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

trait UnsafeMeter:
  def unsafeMark(num: Long): Unit
  final def unsafeMark(num: Int): Unit = unsafeMark(num.toLong)
end UnsafeMeter

object Meter {

  private class Impl[F[_]](
    label: MetricLabel,
    metricRegistry: MetricRegistry,
    squants: Squants,
    name: MetricName)(using F: Sync[F])
      extends Meter[F] with UnsafeMeter {

    private val meterName: String =
      MetricID(
        metricLabel = label,
        metricName = name,
        Category.Meter(kind = MeterKind.Meter, squants = squants)
      ).identifier

    private val meter: CodahaleMeter = metricRegistry.meter(meterName)

    override def mark(num: Long): F[Unit] = F.delay(meter.mark(num))
    override def unsafeMark(num: Long): Unit = meter.mark(num)

    val unregister: F[Unit] = F.delay(metricRegistry.remove(meterName)).void

  }

  final class Builder private[Meter] (isEnabled: Boolean, squants: Squants) extends EnableConfig[Builder] {

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, squants)

    def withUnit[A <: Quantity[A]](um: UnitOfMeasure[A]): Builder =
      new Builder(isEnabled, Squants(um))

    private[Meter] def build[F[_]](label: MetricLabel, name: String, metricRegistry: MetricRegistry)(using
      F: Sync[F]): Resource[F, Meter[F] & UnsafeMeter] = {
      def meter: Resource[F, Meter[F] & UnsafeMeter] =
        Resource.make(MetricName(name).map { metricName =>
          new Impl[F](label = label, metricRegistry = metricRegistry, squants = squants, name = metricName)
        })(_.unregister)

      def noop: Meter[F] & UnsafeMeter = new Meter[F] with UnsafeMeter {
        override def mark(num: Long): F[Unit] = ().pure
        override def unsafeMark(num: Long): Unit = ()
      }

      if isEnabled then meter else noop.pure
    }
  }

  private[metrics] def apply[F[_]: Sync](
    mr: MetricRegistry,
    label: MetricLabel,
    name: String,
    f: Endo[Builder]): Resource[F, Meter[F] & UnsafeMeter] =
    f(new Builder(isEnabled = true, squants = Squants(Each)))
      .build[F](label, name, mr)
}
