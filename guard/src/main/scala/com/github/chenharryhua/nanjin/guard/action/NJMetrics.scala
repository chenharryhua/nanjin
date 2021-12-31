package com.github.chenharryhua.nanjin.guard.action

import cats.Eval
import cats.effect.std.Dispatcher
import com.codahale.metrics.MetricFilter
import com.github.chenharryhua.nanjin.guard.config.MetricSnapshotType
import com.github.chenharryhua.nanjin.guard.event.{EventPublisher, MetricReportType, MetricSnapshot}

class NJMetrics[F[_]](dispatcher: Dispatcher[F], eventPublisher: EventPublisher[F]) {

  def reset: F[Unit]      = eventPublisher.metricsReset(None)
  def unsafeReset(): Unit = dispatcher.unsafeRunSync(reset)

  val snapshotFull: Eval[MetricSnapshot] =
    Eval.always(MetricSnapshot.full(eventPublisher.metricRegistry, eventPublisher.serviceInfo.serviceParams))

  private def reporting(mst: MetricSnapshotType, metricFilter: MetricFilter): F[Unit] =
    eventPublisher.metricsReport(metricFilter, MetricReportType.Adhoc(mst))

  def report(metricFilter: MetricFilter): F[Unit] = reporting(MetricSnapshotType.Regular, metricFilter)
  def unsafeReport(metricFilter: MetricFilter): Unit =
    dispatcher.unsafeRunSync(reporting(MetricSnapshotType.Regular, metricFilter))

  def deltaReport(metricFilter: MetricFilter): F[Unit] = reporting(MetricSnapshotType.Delta, metricFilter)
  def unsafeDeltaReport(metricFilter: MetricFilter): Unit =
    dispatcher.unsafeRunSync(reporting(MetricSnapshotType.Delta, metricFilter))

  def fullReport: F[Unit] = reporting(MetricSnapshotType.Full, MetricFilter.ALL)
  def unsafeFullReport(): Unit =
    dispatcher.unsafeRunSync(reporting(MetricSnapshotType.Full, MetricFilter.ALL))
}
