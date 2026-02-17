package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Sync
import cats.syntax.functor.toFunctorOps
import cats.syntax.flatMap.toFlatMapOps
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsReport
import com.github.chenharryhua.nanjin.guard.event.{Event, MetricIndex, MetricSnapshot, ScrapeMode}
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
  def cheapMetricsReport(tick: Tick): F[MetricsReport]
}

abstract private class AdhocMetricsImpl[F[_]](
  channel: Channel[F, Event],
  eventLogger: EventLogger[F],
  metricRegistry: MetricRegistry)(implicit F: Sync[F])
    extends AdhocMetrics[F] {
  private val sp: ServiceParams = eventLogger.serviceParams

  override val reset: F[Unit] =
    F.realTimeInstant.flatMap(ts =>
      metrics_reset(
        channel = channel,
        eventLogger = eventLogger,
        metricRegistry = metricRegistry,
        index = MetricIndex.Adhoc(sp.toZonedDateTime(ts))
      ))

  override val report: F[Unit] =
    F.realTimeInstant.flatMap(ts =>
      metrics_report(
        channel = channel,
        eventLogger = eventLogger,
        metricRegistry = metricRegistry,
        index = MetricIndex.Adhoc(sp.toZonedDateTime(ts))
      ).void)

  override def cheapMetricsReport(tick: Tick): F[MetricsReport] =
    create_metrics_report(sp, metricRegistry, MetricIndex.Periodic(tick), ScrapeMode.Cheap)

  override val getSnapshot: F[MetricSnapshot] =
    MetricSnapshot.timed[F](metricRegistry, ScrapeMode.Full).map(_._2)
}
