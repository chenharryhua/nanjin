package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.DigestedName

final class NJCounter[F[_]] private[guard] (
  name: DigestedName,
  metricRegistry: MetricRegistry,
  isCountAsError: Boolean
)(implicit F: Sync[F]) {
  private lazy val counter: Counter = metricRegistry.counter(counterMRName(name, isCountAsError))

  def asError: NJCounter[F] = new NJCounter[F](name, metricRegistry, isCountAsError = true)

  def unsafeInc(num: Long): Unit = counter.inc(num)
  def inc(num: Long): F[Unit]    = F.delay(unsafeInc(num))

  def unsafeDec(num: Long): Unit = counter.dec(num)
  def dec(num: Long): F[Unit]    = F.delay(unsafeDec(num))

}
