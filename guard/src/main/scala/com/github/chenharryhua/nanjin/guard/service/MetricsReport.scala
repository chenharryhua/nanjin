package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Sync
import cats.implicits.{toFlatMapOps, toFunctorOps}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{Event, MetricIndex}
import fs2.concurrent.Channel

abstract class MetricsReport[F[_]] private[service] (
  channel: Channel[F, Event],
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry)(implicit F: Sync[F]) {

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
          index = MetricIndex.Adhoc(serviceParams.toZonedDateTime(ts))
        )
        .void)
}
