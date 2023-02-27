package com.github.chenharryhua.nanjin.guard.service

import cats.Monad
import cats.effect.kernel.Clock
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, NJEvent}
import fs2.concurrent.Channel

final class NJMetrics[F[_]: Clock: Monad] private[service] (
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry) {

  def reset: F[Unit] =
    publisher.metricReset(channel, serviceParams, metricRegistry, MetricIndex.Adhoc)

  def report: F[Unit] =
    publisher.metricReport(channel, serviceParams, metricRegistry, MetricIndex.Adhoc)

}
