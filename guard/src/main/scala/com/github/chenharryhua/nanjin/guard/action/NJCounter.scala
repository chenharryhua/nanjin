package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.MetricName

final class NJCounter[F[_]](
  metricName: MetricName,
  metricRegistry: MetricRegistry,
  isCountAsError: Boolean
)(implicit F: Sync[F]) {
  private val name: String = counterMRName(metricName, isCountAsError)

  def asError: NJCounter[F] = new NJCounter[F](metricName, metricRegistry, isCountAsError = true)

  def unsafeInc(num: Long): Unit = metricRegistry.counter(name).inc(num)
  def inc(num: Long): F[Unit]    = F.delay(unsafeInc(num))

  def unsafeDec(num: Long): Unit = metricRegistry.counter(name).dec(num)
  def dec(num: Long): F[Unit]    = F.delay(unsafeDec(num))

}
