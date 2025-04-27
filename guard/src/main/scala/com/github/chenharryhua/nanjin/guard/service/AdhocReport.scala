package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Sync
import cats.implicits.{toFlatMapOps, toFunctorOps}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{Event, MetricIndex}
import fs2.concurrent.Channel

sealed trait AdhocReport[F[_]] {
  def reset: F[Unit]
  def report: F[Unit]
}

abstract private class AdhocReportImpl[F[_]](
  channel: Channel[F, Event],
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry)(implicit F: Sync[F])
    extends AdhocReport[F] {

  override def reset: F[Unit] =
    F.realTimeInstant.flatMap(ts =>
      metricReset(
        channel = channel,
        serviceParams = serviceParams,
        metricRegistry = metricRegistry,
        index = MetricIndex.Adhoc(serviceParams.toZonedDateTime(ts))))

  override def report: F[Unit] =
    F.realTimeInstant.flatMap(ts =>
      metricReport(
        channel = channel,
        serviceParams = serviceParams,
        metricRegistry = metricRegistry,
        index = MetricIndex.Adhoc(serviceParams.toZonedDateTime(ts))
      ).void)
}
