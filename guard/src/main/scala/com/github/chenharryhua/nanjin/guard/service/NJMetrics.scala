package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Async, Ref}
import cats.implicits.toFunctorOps
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.MetricSnapshotType
import com.github.chenharryhua.nanjin.guard.event.{MetricReportType, MetricSnapshot, NJEvent}
import fs2.concurrent.Channel

final class NJMetrics[F[_]: Async] private[service] (
  channel: Channel[F, NJEvent],
  metricRegistry: MetricRegistry,
  serviceStatus: Ref[F, ServiceStatus]) {

  def reset: F[Unit] = publisher.metricReset(channel, serviceStatus, metricRegistry, None)

  private def reporting(mst: MetricSnapshotType, metricFilter: MetricFilter): F[Unit] =
    publisher.metricReport(channel, serviceStatus, metricRegistry, metricFilter, MetricReportType.Adhoc(mst))

  def report(metricFilter: MetricFilter): F[Unit] = reporting(MetricSnapshotType.Regular, metricFilter)

  def deltaReport(metricFilter: MetricFilter): F[Unit] = reporting(MetricSnapshotType.Delta, metricFilter)

  def fullReport: F[Unit] = reporting(MetricSnapshotType.Full, MetricFilter.ALL)

  // query
  def snapshot: F[MetricSnapshot] =
    serviceStatus.get.map(ss => MetricSnapshot.full(metricRegistry, ss.serviceParams))

  def snapshot(metricFilter: MetricFilter): F[MetricSnapshot] =
    serviceStatus.get.map(ss => MetricSnapshot.regular(metricFilter, metricRegistry, ss.serviceParams))

}
