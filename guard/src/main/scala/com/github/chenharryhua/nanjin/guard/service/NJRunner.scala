package com.github.chenharryhua.nanjin.guard.service

import cats.Applicative
import cats.effect.kernel.Async
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.action.{NJExecutor, NJMessenger}
import com.github.chenharryhua.nanjin.guard.config.{MetricName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.metrics.NJMetrics
import fs2.concurrent.Channel

sealed trait NJRunner[F[_]] {
  def executor: NJExecutor[F]
  def messenger: NJMessenger[F]
  def metrics: NJMetrics[F]
}

object NJRunner {
  def dummy[F[_]: Applicative]: NJRunner[F] = new NJRunner[F] {
    override val executor: NJExecutor[F]   = NJExecutor.dummy[F]
    override val messenger: NJMessenger[F] = NJMessenger.dummy[F]
    override val metrics: NJMetrics[F]     = NJMetrics.dummy[F]
  }

  final class Builder(isEnabled: Boolean, policy: Policy) extends EnableConfig[Builder] {

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, policy)

    def withPolicy(policy: Policy): Builder =
      new Builder(isEnabled, policy)

    def build[F[_]: Async](
      metricName: MetricName,
      serviceParams: ServiceParams,
      metricRegistry: MetricRegistry,
      channel: Channel[F, NJEvent]
    ): NJRunner[F] =
      if (isEnabled) {
        new NJRunner[F] {
          override val executor: NJExecutor[F] =
            new NJExecutor.Builder(isEnabled, policy).build(serviceParams.initialStatus)
          override val messenger: NJMessenger[F] =
            new NJMessenger.Builder(isEnabled).build[F](metricName, channel, serviceParams)
          override val metrics: NJMetrics[F] =
            new NJMetrics.Builder(isEnabled).build[F](metricRegistry, metricName)
        }
      } else dummy[F]
  }
}
