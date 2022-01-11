package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.codahale.metrics.{Counter, Meter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.DigestedName

// counter can be reset, meter can't
final class NJMeter[F[_]] private[guard] (
  metricName: DigestedName,
  metricRegistry: MetricRegistry,
  isCountAsError: Boolean,
  isCounting: Boolean)(implicit F: Sync[F]) {

  private lazy val meter: Meter     = metricRegistry.meter(meterMRName(metricName))
  private lazy val counter: Counter = metricRegistry.counter(counterMRName(metricName, isCountAsError))

  def asError   = new NJMeter[F](metricName, metricRegistry, true, true)
  def withCount = new NJMeter[F](metricName, metricRegistry, false, true)

  def unsafeMark(num: Long): Unit = meter.mark(num)
  def mark(num: Long): F[Unit] =
    for {
      _ <- F.delay(unsafeMark(num))
      _ <- F.delay(counter.inc(num)).whenA(isCounting)
    } yield ()
}
