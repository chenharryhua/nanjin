package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.MetricName

final class NJMeter[F[_]](metricName: MetricName, metricRegistry: MetricRegistry, isCountAsError: Boolean)(implicit
  F: Sync[F]) {

  private val name: String = meterMRName(metricName, isCountAsError)

  def asError: NJMeter[F] = new NJMeter[F](metricName, metricRegistry, isCountAsError = true)

  def unsafeMark(num: Long): Unit = metricRegistry.meter(name).mark(num)
  def mark(num: Long): F[Unit]    = F.delay(unsafeMark(num))
}
