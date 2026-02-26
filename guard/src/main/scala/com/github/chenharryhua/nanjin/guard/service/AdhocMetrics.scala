package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Sync
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index.{Adhoc, Periodic}
import com.github.chenharryhua.nanjin.guard.event.{Event, ScrapeMode, Snapshot}
import com.github.chenharryhua.nanjin.guard.logging.LogSink
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
  def getSnapshot: F[Snapshot]
  def cheapSnapshot(tick: Tick): F[MetricsSnapshot]
}

abstract private class AdhocMetricsImpl[F[_]](
  serviceParams: ServiceParams,
  channel: Channel[F, Event],
  logSink: LogSink[F],
  metricRegistry: MetricRegistry)(implicit F: Sync[F])
    extends AdhocMetrics[F] {

  override val reset: F[Unit] =
    F.realTimeInstant.flatMap(ts =>
      publish_metrics_reset(
        serviceParams = serviceParams,
        channel = channel,
        logSink = logSink,
        metricRegistry = metricRegistry,
        index = Adhoc(serviceParams.toZonedDateTime(ts))
      ).void)

  override val report: F[Unit] =
    F.realTimeInstant.flatMap(ts =>
      publish_metrics_report(
        serviceParams = serviceParams,
        channel = channel,
        logSink = logSink,
        metricRegistry = metricRegistry,
        index = Adhoc(serviceParams.toZonedDateTime(ts))
      ).void)

  override def cheapSnapshot(tick: Tick): F[MetricsSnapshot] =
    create_metrics_report(serviceParams, metricRegistry, Periodic(tick), ScrapeMode.Cheap)

  override val getSnapshot: F[Snapshot] =
    Snapshot.timed[F](metricRegistry, ScrapeMode.Full).map(_._2)
}
