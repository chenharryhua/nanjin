package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Counter, Meter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.Digested

// counter can be reset, meter can't
final class NJMeter[F[_]] private[guard] (
  digested: Digested,
  metricRegistry: MetricRegistry,
  isCounting: Boolean)(implicit F: Sync[F]) {

  private lazy val meter: Meter     = metricRegistry.meter(meterMRName(digested))
  private lazy val counter: Counter = metricRegistry.counter(counterMRName(digested, asError = false))

  def withCounting: NJMeter[F] = new NJMeter[F](digested, metricRegistry, true)

  def mark(num: Long): F[Unit] = F.delay {
    meter.mark(num)
    if (isCounting) counter.inc(num)
  }
}
