package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.MetricName

final class NJMeter[F[_]: Sync](metricName: MetricName, metricRegistry: MetricRegistry, isCountAsError: Boolean) {

  private val name: String = meterMRName(metricName, isCountAsError)

  def asError: NJMeter[F] = new NJMeter[F](metricName, metricRegistry, isCountAsError = true)

  def unsafeMark(n: Long): Unit = metricRegistry.meter(name).mark(n)
  def mark(n: Long): F[Unit]    = Sync[F].delay(unsafeMark(n))
}
