package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Counter, Meter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{CountAction, DigestedName}

// counter can be reset, meter can't
final class NJMeter[F[_]] private[guard] (
  metricName: DigestedName,
  metricRegistry: MetricRegistry,
  isCounting: CountAction)(implicit F: Sync[F]) {

  private lazy val meter: Meter     = metricRegistry.meter(meterMRName(metricName))
  private lazy val counter: Counter = metricRegistry.counter(counterMRName(metricName, asError = false))

  def withCounting: NJMeter[F] = new NJMeter[F](metricName, metricRegistry, CountAction.Yes)

  def unsafeMark(num: Long): Unit = {
    meter.mark(num)
    if (isCounting.value) counter.inc(num)
  }
  def mark(num: Long): F[Unit] = F.delay(unsafeMark(num))
}
