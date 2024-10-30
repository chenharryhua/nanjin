package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Async
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.action.{NJMessenger, NJRetry}
import com.github.chenharryhua.nanjin.guard.config.{MetricName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.metrics.NJMetrics
import fs2.concurrent.Channel

sealed trait NJFacilitator[F[_]] {
  def retry: NJRetry[F]
  def messenger: NJMessenger[F]
  def metrics: NJMetrics[F]
}

object NJFacilitator {

  final class Builder private[service] (policy: Policy) {

    def withPolicy(policy: Policy): Builder =
      new Builder(policy)

    private[service] def build[F[_]: Async](
      metricName: MetricName,
      serviceParams: ServiceParams,
      metricRegistry: MetricRegistry,
      channel: Channel[F, NJEvent]
    ): NJFacilitator[F] = new NJFacilitator[F] {
      override val retry: NJRetry[F] =
        new NJRetry.Impl[F](serviceParams.initialStatus.renewPolicy(policy))
      override val messenger: NJMessenger[F] =
        new NJMessenger.Impl[F](metricName, serviceParams, channel)
      override val metrics: NJMetrics[F] =
        new NJMetrics.Impl[F](metricName, metricRegistry, isEnabled = true)
    }
  }
}
