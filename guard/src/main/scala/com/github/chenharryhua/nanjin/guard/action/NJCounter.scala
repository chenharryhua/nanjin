package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.MetricName

final class NJCounter[F[_]: Sync](
  metricName: MetricName,
  metricRegistry: MetricRegistry,
  isCountAsError: Boolean
) {
  def asError: NJCounter[F] = new NJCounter[F](metricName, metricRegistry, isCountAsError = true)

  private val name: String = counterMRName(metricName, isCountAsError)

  def unsafeIncrease(num: Long): Unit = metricRegistry.counter(name).inc()
  def increase(num: Long): F[Unit]    = Sync[F].delay(unsafeIncrease(num))

  def unsafeReplace(num: Long): Unit = {
    val old = metricRegistry.counter(name).getCount
    metricRegistry.counter(name).inc(num)
    metricRegistry.counter(name).dec(old)
  }
  def replace(num: Long): F[Unit] = Sync[F].delay(unsafeReplace(num))

}
