package com.github.chenharryhua.nanjin.guard.service

import cats.MonadError
import cats.effect.kernel.Clock
import cats.implicits.{toFlatMapOps, toFunctorOps}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, NJEvent}
import fs2.concurrent.Channel

abstract class NJMetrics[F[_]] private[service] (
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry)(implicit F: Clock[F], M: MonadError[F, Throwable]) {

  def reset: F[Unit] =
    F.realTimeInstant.flatMap(ts =>
      publisher.metricReset(
        channel = channel,
        serviceParams = serviceParams,
        metricRegistry = metricRegistry,
        index = MetricIndex.Adhoc(serviceParams.toZonedDateTime(ts))))

  def report: F[Unit] =
    F.realTimeInstant.flatMap(ts =>
      publisher
        .metricReport(
          channel = channel,
          serviceParams = serviceParams,
          metricRegistry = metricRegistry,
          index = MetricIndex.Adhoc(serviceParams.toZonedDateTime(ts)))
        .void)
}
