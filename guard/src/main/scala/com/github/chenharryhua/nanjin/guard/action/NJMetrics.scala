package com.github.chenharryhua.nanjin.guard.action

import cats.Eval
import cats.effect.std.Dispatcher
import com.codahale.metrics.MetricFilter
import com.github.chenharryhua.nanjin.guard.config.MetricSnapshotType
import com.github.chenharryhua.nanjin.guard.event.{EventPublisher, MetricReportType, MetricSnapshot}

class NJMetrics[F[_]](dispatcher: Dispatcher[F], eventPublisher: EventPublisher[F], metricFilter: MetricFilter) {
  def withMetricFilter(metricFilter: MetricFilter): NJMetrics[F] =
    new NJMetrics[F](dispatcher, eventPublisher, metricFilter)

  def reset: F[Unit]      = eventPublisher.metricsReset(metricFilter, None)
  def unsafeReset(): Unit = dispatcher.unsafeRunSync(reset)

  val snapshotFull: Eval[MetricSnapshot] =
    Eval.always(MetricSnapshot.Full(eventPublisher.metricRegistry, eventPublisher.serviceInfo.serviceParams))

  private def reporting(mst: MetricSnapshotType): F[Unit] =
    eventPublisher.metricsReport(metricFilter, MetricReportType.Adhoc(mst))

  def report: F[Unit]      = reporting(MetricSnapshotType.Positive)
  def unsafeReport(): Unit = dispatcher.unsafeRunSync(reporting(MetricSnapshotType.Positive))

  def deltaReport: F[Unit]      = reporting(MetricSnapshotType.Delta)
  def unsafeDeltaReport(): Unit = dispatcher.unsafeRunSync(reporting(MetricSnapshotType.Delta))

  def fullReport: F[Unit]      = reporting(MetricSnapshotType.Full)
  def unsafeFullReport(): Unit = dispatcher.unsafeRunSync(reporting(MetricSnapshotType.Full))
}
