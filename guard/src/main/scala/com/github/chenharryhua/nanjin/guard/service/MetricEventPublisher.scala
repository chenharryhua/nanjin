package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Ref, RefSource, Temporal}
import cats.syntax.all.*
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{MetricSnapshotType, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import fs2.concurrent.Channel

import scala.jdk.CollectionConverters.CollectionHasAsScala

final private class MetricEventPublisher[F[_]](
  serviceParams: ServiceParams,
  channel: Channel[F, NJEvent],
  metricRegistry: MetricRegistry,
  serviceStatus: RefSource[F, ServiceStatus],
  ongoings: RefSource[F, Set[ActionInfo]],
  lastCountersRef: Ref[F, MetricSnapshot.LastCounters]
)(implicit F: Temporal[F]) {

  def metricsReport(metricFilter: MetricFilter, metricReportType: MetricReportType): F[Unit] =
    for {
      ts <- F.realTimeInstant
      ogs <- ongoings.get
      oldLast <- lastCountersRef.getAndSet(MetricSnapshot.LastCounters(metricRegistry))
      ss <- serviceStatus.get
      _ <- channel.send(
        MetricsReport(
          serviceStatus = ss,
          reportType = metricReportType,
          ongoings = ogs.map(OngoingAction(_)).toList.sortBy(_.launchTime),
          timestamp = ts,
          serviceParams = serviceParams,
          snapshot = metricReportType.snapshotType match {
            case MetricSnapshotType.Full =>
              MetricSnapshot.full(metricRegistry, serviceParams)
            case MetricSnapshotType.Regular =>
              MetricSnapshot.regular(metricFilter, metricRegistry, serviceParams)
            case MetricSnapshotType.Delta =>
              MetricSnapshot.delta(oldLast, metricFilter, metricRegistry, serviceParams)
          }
        ))
    } yield ()

  /** Reset Counters only
    */
  def metricsReset(cronExpr: Option[CronExpr]): F[Unit] =
    for {
      ts <- F.realTimeInstant
      ss <- serviceStatus.get
      msg = cronExpr.flatMap { ce =>
        ce.next(ts).map { next =>
          MetricsReset(
            resetType = MetricResetType.Scheduled(next),
            serviceStatus = ss,
            timestamp = ts,
            serviceParams = serviceParams,
            snapshot = MetricSnapshot.regular(MetricFilter.ALL, metricRegistry, serviceParams)
          )
        }
      }.getOrElse(
        MetricsReset(
          resetType = MetricResetType.Adhoc,
          serviceStatus = ss,
          timestamp = ts,
          serviceParams = serviceParams,
          snapshot = MetricSnapshot.full(metricRegistry, serviceParams)
        ))
      _ <- channel.send(msg)
      _ <- lastCountersRef.set(MetricSnapshot.LastCounters.empty)
    } yield metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount))

}
