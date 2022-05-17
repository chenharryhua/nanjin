package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Counter, Meter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{CountAction, Digested}

// counter can be reset, meter can't
final class NJMeter[F[_]] private[guard] (
  metricName: Digested,
  metricRegistry: MetricRegistry,
  isCounting: CountAction)(implicit F: Sync[F]) {

  private lazy val meter: Meter     = metricRegistry.meter(meterMRName(metricName))
  private lazy val counter: Counter = metricRegistry.counter(counterMRName(metricName, asError = false))

  def withCounting: NJMeter[F] = new NJMeter[F](metricName, metricRegistry, CountAction.Yes)

  def mark(num: Long): F[Unit] = F.delay {
    meter.mark(num)
    if (isCounting.value) counter.inc(num)
  }
}
