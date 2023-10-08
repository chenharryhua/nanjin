package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import cats.implicits.toFunctorOps
import com.codahale.metrics.{Counter, Meter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{Category, CounterKind, MeterKind, MetricID, MetricName}
import com.github.chenharryhua.nanjin.guard.event.MeasurementUnit

// counter can be reset, meter can't
final class NJMeter[F[_]] private[guard] (
  name: MetricName,
  metricRegistry: MetricRegistry,
  unit: MeasurementUnit,
  isCounting: Boolean)(implicit F: Sync[F]) {
  private val meterName   = MetricID(name, Category.Meter(MeterKind.Dropwizard, unit)).identifier
  private val counterName = MetricID(name, Category.Counter(CounterKind.MeterCounter)).identifier

  private lazy val meter: Meter     = metricRegistry.meter(meterName)
  private lazy val counter: Counter = metricRegistry.counter(counterName)

  private[guard] def unregister: F[Unit] = F.blocking {
    metricRegistry.remove(meterName)
    metricRegistry.remove(counterName)
  }.void

  def counted: NJMeter[F] = new NJMeter[F](name, metricRegistry, unit, true)

  def unsafeMark(num: Long): Unit = {
    meter.mark(num)
    if (isCounting) counter.inc(num)
  }

  def mark(num: Long): F[Unit] = F.delay(unsafeMark(num))

}
