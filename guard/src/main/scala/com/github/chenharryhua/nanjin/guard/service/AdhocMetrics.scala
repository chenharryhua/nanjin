package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Sync
import cats.implicits.{toFlatMapOps, toFunctorOps}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{Event, MetricIndex, MetricSnapshot}
import fs2.concurrent.Channel

/** adhoc metrics report and reset
  */
sealed trait AdhocMetrics[F[_]] {

  /** reset all counters to zero
    */
  def reset: F[Unit]

  /** report current metrics
    */
  def report: F[Unit]
  def getSnapshot: F[MetricSnapshot]
}

abstract private class AdhocMetricsImpl[F[_]](
  channel: Channel[F, Event],
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry)(implicit F: Sync[F])
    extends AdhocMetrics[F] {

  override val reset: F[Unit] =
    F.realTimeInstant.flatMap(ts =>
      metricReset(
        channel = channel,
        serviceParams = serviceParams,
        metricRegistry = metricRegistry,
        index = MetricIndex.Adhoc(serviceParams.toZonedDateTime(ts))))

  override val report: F[Unit] =
    F.realTimeInstant.flatMap(ts =>
      metricReport(
        channel = channel,
        serviceParams = serviceParams,
        metricRegistry = metricRegistry,
        index = MetricIndex.Adhoc(serviceParams.toZonedDateTime(ts))
      ).void)

  override val getSnapshot: F[MetricSnapshot] =
    MetricSnapshot.timed(metricRegistry).map(_._2)
}
