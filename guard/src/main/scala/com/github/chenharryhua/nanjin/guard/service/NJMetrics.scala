package com.github.chenharryhua.nanjin.guard.service

import cats.MonadError
import cats.effect.kernel.Clock
import cats.implicits.toFlatMapOps
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, NJEvent}
import fs2.concurrent.Channel

final class NJMetrics[F[_]] private[service] (
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry)(implicit F: Clock[F], M: MonadError[F, Throwable]) {

  def reset: F[Unit] = F.realTimeInstant.flatMap(ts =>
    publisher.metricReset(channel, serviceParams, metricRegistry, MetricIndex.Adhoc, ts))

  def report: F[Unit] = F.realTimeInstant.flatMap(ts =>
    publisher.metricReport(channel, serviceParams, metricRegistry, MetricIndex.Adhoc, ts))

}
