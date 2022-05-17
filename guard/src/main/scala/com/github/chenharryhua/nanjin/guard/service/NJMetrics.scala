package com.github.chenharryhua.nanjin.guard.service

import com.codahale.metrics.MetricFilter
import com.github.chenharryhua.nanjin.guard.config.MetricSnapshotType
import com.github.chenharryhua.nanjin.guard.event.{MetricReportType, MetricSnapshot}

final class NJMetrics[F[_]] private[service] (publisher: MetricEventPublisher[F]) {

  def reset: F[Unit] = publisher.metricsReset(None)

  private def reporting(mst: MetricSnapshotType, metricFilter: MetricFilter): F[Unit] =
    publisher.metricsReport(metricFilter, MetricReportType.Adhoc(mst))

  def report(metricFilter: MetricFilter): F[Unit] = reporting(MetricSnapshotType.Regular, metricFilter)

  def deltaReport(metricFilter: MetricFilter): F[Unit] = reporting(MetricSnapshotType.Delta, metricFilter)

  def fullReport: F[Unit] = reporting(MetricSnapshotType.Full, MetricFilter.ALL)

  // query
  def snapshot: F[MetricSnapshot]                             = publisher.snapshotFull
  def snapshot(metricFilter: MetricFilter): F[MetricSnapshot] = publisher.snapshot(metricFilter)

}
