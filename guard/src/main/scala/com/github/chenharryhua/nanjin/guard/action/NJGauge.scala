package com.github.chenharryhua.nanjin.guard.action

import cats.Show
import cats.effect.kernel.{Resource, Sync}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.Digested
import org.apache.commons.lang3.exception.ExceptionUtils

final class NJGauge[F[_]] private[guard] (
  digested: Digested,
  metricRegistry: MetricRegistry,
  dispatcher: Dispatcher[F])(implicit F: Sync[F]) {

  def register[A: Show](value: F[A]): Resource[F, Unit] =
    Resource
      .make(F.delay {
        val name = gaugeMRName(digested)
        metricRegistry.gauge(
          name,
          () =>
            new Gauge[String] {
              override def getValue: String =
                dispatcher.unsafeRunSync(value.attempt).fold(ExceptionUtils.getMessage, _.show)
            }
        )
        name
      })(name => F.delay(metricRegistry.remove(name)).void)
      .void

  def register[A: Show](value: => A): Resource[F, Unit] = register(F.delay(value))
}
