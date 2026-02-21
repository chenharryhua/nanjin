package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Sync
import cats.syntax.functor.toFunctorOps
import cats.syntax.flatMap.toFlatMapOps
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsReport
import com.github.chenharryhua.nanjin.guard.event.{Event, ScrapeMode, Snapshot}
import fs2.concurrent.Channel
import com.github.chenharryhua.nanjin.guard.event.Index

/** adhoc metrics report and reset
  */
sealed trait AdhocMetrics[F[_]] {

  /** reset all counters to zero
    */
  def reset: F[Unit]

  /** report current metrics
    */
  def report: F[Unit]
  def getSnapshot: F[Snapshot]
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
      publish_metrics_reset(
        channel = channel,
        eventLogger = eventLogger,
        metricRegistry = metricRegistry,
        index = Index.Adhoc(sp.toZonedDateTime(ts))
      ))

  override val report: F[Unit] =
    F.realTimeInstant.flatMap(ts =>
      publish_metrics_report(
        channel = channel,
        eventLogger = eventLogger,
        metricRegistry = metricRegistry,
        index = Index.Adhoc(sp.toZonedDateTime(ts))
      ).void)

  override def cheapMetricsReport(tick: Tick): F[MetricsReport] =
    create_metrics_report(sp, metricRegistry, Index.Periodic(tick), ScrapeMode.Cheap)

  override val getSnapshot: F[Snapshot] =
    Snapshot.timed[F](metricRegistry, ScrapeMode.Full).map(_._2)
}
