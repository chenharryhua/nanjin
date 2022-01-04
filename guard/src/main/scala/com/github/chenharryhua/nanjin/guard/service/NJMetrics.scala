package com.github.chenharryhua.nanjin.guard.service

import cats.Eval
import cats.effect.kernel.{Ref, RefSource, Temporal}
import cats.effect.std.Dispatcher
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{MetricSnapshotType, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, MetricReportType, MetricSnapshot, NJEvent, ServiceStatus}
import fs2.concurrent.Channel

final class NJMetrics[F[_]: Temporal] private[service] (
  dispatcher: Dispatcher[F],
  serviceParams: ServiceParams,
  channel: Channel[F, NJEvent],
  metricRegistry: MetricRegistry,
  serviceStatus: RefSource[F, ServiceStatus],
  ongoings: RefSource[F, Set[ActionInfo]],
  lastCounters: Ref[F, MetricSnapshot.LastCounters]
) {
  private val metricEventPublisher: MetricEventPublisher[F] =
    new MetricEventPublisher[F](serviceParams, channel, metricRegistry, serviceStatus, ongoings, lastCounters)

  def reset: F[Unit]      = metricEventPublisher.metricsReset(None)
  def unsafeReset(): Unit = dispatcher.unsafeRunSync(reset)

  private def reporting(mst: MetricSnapshotType, metricFilter: MetricFilter): F[Unit] =
    metricEventPublisher.metricsReport(metricFilter, MetricReportType.Adhoc(mst))

  def report(metricFilter: MetricFilter): F[Unit] = reporting(MetricSnapshotType.Regular, metricFilter)
  def unsafeReport(metricFilter: MetricFilter): Unit =
    dispatcher.unsafeRunSync(reporting(MetricSnapshotType.Regular, metricFilter))

  def deltaReport(metricFilter: MetricFilter): F[Unit] = reporting(MetricSnapshotType.Delta, metricFilter)
  def unsafeDeltaReport(metricFilter: MetricFilter): Unit =
    dispatcher.unsafeRunSync(reporting(MetricSnapshotType.Delta, metricFilter))

  def fullReport: F[Unit] = reporting(MetricSnapshotType.Full, MetricFilter.ALL)
  def unsafeFullReport(): Unit =
    dispatcher.unsafeRunSync(reporting(MetricSnapshotType.Full, MetricFilter.ALL))

  // query metricRegistry
  val snapshotFull: Eval[MetricSnapshot] =
    Eval.always(MetricSnapshot.full(metricRegistry, serviceParams))

  def snapshot(metricFilter: MetricFilter): Eval[MetricSnapshot] =
    Eval.always(MetricSnapshot.regular(metricFilter, metricRegistry, serviceParams))

}
