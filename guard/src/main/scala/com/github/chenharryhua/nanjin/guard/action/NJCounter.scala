package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.Digested

final class NJCounter[F[_]] private[guard] (metricName: Digested, metricRegistry: MetricRegistry, isError: Boolean)(
  implicit F: Sync[F]) {

  private lazy val counter: Counter = metricRegistry.counter(counterMRName(metricName, isError))

  def asError: NJCounter[F] = new NJCounter[F](metricName, metricRegistry, isError = true)

  def inc(num: Long): F[Unit] = F.delay(counter.inc(num))

  def dec(num: Long): F[Unit] = F.delay(counter.dec(num))

}
