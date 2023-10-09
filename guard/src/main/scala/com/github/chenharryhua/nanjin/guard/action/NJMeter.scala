package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import cats.implicits.toFunctorOps
import com.codahale.metrics.{Counter, Meter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{Category, CounterKind, MeterKind, MetricID, MetricName}
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, NJDimensionlessUnit, NJInformationUnit}
import squants.information.Information

sealed abstract class AbstractMeter[F[_]](
  name: MetricName,
  metricRegistry: MetricRegistry,
  isCounting: Boolean,
  unit: MeasurementUnit)(implicit F: Sync[F]) {
  private val meterName: String =
    MetricID(name, Category.Meter(MeterKind.Dropwizard, unit)).identifier

  private val counterName: String =
    MetricID(name, Category.Counter(CounterKind.MeterCounter)).identifier

  private lazy val meter: Meter     = metricRegistry.meter(meterName)
  private lazy val counter: Counter = metricRegistry.counter(counterName)

  final def unregister: F[Unit] = F.blocking {
    metricRegistry.remove(meterName)
    metricRegistry.remove(counterName)
  }.void

  final def unsafeMark(num: Long): Unit = {
    meter.mark(num)
    if (isCounting) counter.inc(num)
  }
  final def mark(num: Long): F[Unit] = F.delay(unsafeMark(num))
}

final class NJInfoMeter[F[_]: Sync] private[guard] (
  name: MetricName,
  metricRegistry: MetricRegistry,
  isCounting: Boolean,
  unit: NJInformationUnit)
    extends AbstractMeter(name, metricRegistry, isCounting, unit) {

  def counted: NJInfoMeter[F] = new NJInfoMeter[F](name, metricRegistry, true, unit)

  def mark(quantity: Information): F[Unit] = mark(quantity.in(unit.mUnit).value.toLong)
}

final class NJCountMeter[F[_]: Sync] private[guard] (
  name: MetricName,
  metricRegistry: MetricRegistry,
  isCounting: Boolean)
    extends AbstractMeter(name, metricRegistry, isCounting, NJDimensionlessUnit.COUNT) {

  def counted: NJCountMeter[F] = new NJCountMeter[F](name, metricRegistry, true)
}
