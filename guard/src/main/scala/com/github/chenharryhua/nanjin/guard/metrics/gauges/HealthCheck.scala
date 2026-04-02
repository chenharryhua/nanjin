package com.github.chenharryhua.nanjin.guard.metrics.gauges

import cats.effect.kernel.{Async, Resource, Temporal}
import cats.effect.syntax.temporal.genTemporalOps
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.Policy

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object HealthCheck {
  final class Builder private[HealthCheck] (
    isEnabled: Boolean,
    timeout: FiniteDuration,
    policy: Option[Policy])
      extends EnableConfig[Builder] {
    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, timeout, policy)

    def withTimeout(timeout: FiniteDuration): Builder =
      new Builder(isEnabled, timeout, policy)

    def withPolicy(f: Policy.type => Policy): Builder =
      new Builder(isEnabled, timeout, Some(f(Policy)))

    def register[F[_]](fb: F[Boolean])(using F: Temporal[F]): Registered[F] =
      Registered[F](
        _.withKind(_.HealthCheck)
          .enable(isEnabled)
          .withTimeout(timeout.plus(timeout)) // avoid race condition
          .withPolicy(policy)
          .register(F.handleError(fb.timeout(timeout))(_ => false)))
  }

  final class Registered[F[_]] private[HealthCheck] (
    private[HealthCheck] val build: Gauge.Builder => Gauge.Registered[F]
  )

  private[metrics] def apply[F[_]: Async](
    gp: GaugeParams[F],
    name: String,
    f: Builder => Registered[F]): Resource[F, Unit] =
    Gauge(gp, name, f(new Builder(true, 5.seconds, None)).build)

}
