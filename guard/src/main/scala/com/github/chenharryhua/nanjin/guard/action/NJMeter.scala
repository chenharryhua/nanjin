package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import cats.implicits.toFunctorOps
import com.codahale.metrics.{Counter, Meter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{Category, CounterKind, MeterKind, MetricID, MetricName}
import com.github.chenharryhua.nanjin.guard.event.MeasurementUnit

final class NJMeter[F[_]](
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

  def counted: NJMeter[F] = new NJMeter[F](name, metricRegistry, true, unit)

  def unregister: F[Unit] = F.blocking {
    metricRegistry.remove(meterName)
    metricRegistry.remove(counterName)
  }.void

  def unsafeMark(num: Long): Unit = {
    meter.mark(num)
    if (isCounting) counter.inc(num)
  }
  def mark(num: Long): F[Unit] = F.delay(unsafeMark(num))
}
