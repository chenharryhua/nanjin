package com.github.chenharryhua.nanjin.guard.action

import cats.Show
import cats.effect.kernel.Sync
import cats.effect.std.Dispatcher
import cats.syntax.applicativeError.*
import cats.syntax.show.*
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.Digested
import org.apache.commons.lang3.exception.ExceptionUtils

final class NJGauge[F[_]] private[guard] (
  digested: Digested,
  metricRegistry: MetricRegistry,
  dispatcher: Dispatcher[F])(implicit F: Sync[F]) {

  def register[A: Show](value: F[A]): Gauge[String] =
    metricRegistry.gauge(
      gaugeMRName(digested),
      () =>
        new Gauge[String] {
          override def getValue: String =
            dispatcher.unsafeRunSync(value.attempt).fold(ExceptionUtils.getMessage, _.show)
        })

  def register[A: Show](value: => A): Gauge[String] = register(F.delay(value))
}
