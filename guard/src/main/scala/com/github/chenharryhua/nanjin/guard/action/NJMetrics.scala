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

  def report(mst: MetricSnapshotType): F[Unit] = eventPublisher.metricsReport(metricFilter, MetricReportType.Adhoc(mst))
  def unsafeReport(mst: MetricSnapshotType): Unit = dispatcher.unsafeRunSync(report(mst))

  def report: F[Unit]      = report(MetricSnapshotType.AsIs)
  def unsafeReport(): Unit = dispatcher.unsafeRunSync(report(MetricSnapshotType.AsIs))

  def reportDelta: F[Unit]      = report(MetricSnapshotType.Delta)
  def unsafeReportDelta(): Unit = dispatcher.unsafeRunSync(report(MetricSnapshotType.Delta))

  def reportFull: F[Unit]      = report(MetricSnapshotType.Full)
  def unsafeReportFull(): Unit = dispatcher.unsafeRunSync(report(MetricSnapshotType.Full))

}
