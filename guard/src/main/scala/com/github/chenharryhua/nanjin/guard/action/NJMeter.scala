package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Meter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.DigestedName

final class NJMeter[F[_]: Sync](name: DigestedName, metricRegistry: MetricRegistry, isCountAsError: Boolean) {

  private lazy val meter: Meter = metricRegistry.meter(meterMRName(name, isCountAsError))

  def asError: NJMeter[F] = new NJMeter[F](name, metricRegistry, isCountAsError = true)

  def unsafeMark(num: Long): Unit = meter.mark(num)
  def mark(num: Long): F[Unit]    = Sync[F].delay(unsafeMark(num))
}
