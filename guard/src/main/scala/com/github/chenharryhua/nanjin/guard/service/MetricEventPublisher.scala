package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Async, Ref, RefSource}
import cats.syntax.all.*
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.MetricSnapshotType
import com.github.chenharryhua.nanjin.guard.event.*
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import fs2.concurrent.Channel

import scala.jdk.CollectionConverters.CollectionHasAsScala

final private class MetricEventPublisher[F[_]](
  channel: Channel[F, NJEvent],
  metricRegistry: MetricRegistry,
  serviceStatus: RefSource[F, ServiceStatus],
  ongoings: RefSource[F, Set[ActionInfo]],
  lastCounters: Ref[F, MetricSnapshot.LastCounters])(implicit F: Async[F]) {

  def metricsReport(metricFilter: MetricFilter, metricReportType: MetricReportType): F[Unit] =
    for {
      ss <- serviceStatus.get
      ts <- F.realTimeInstant.map(ss.serviceParams.toZonedDateTime)
      ogs <- ongoings.get
      oldLast <- lastCounters.getAndSet(MetricSnapshot.LastCounters(metricRegistry))
      _ <- channel.send(
        MetricReport(
          serviceStatus = ss,
          reportType = metricReportType,
          ongoings = ogs.map(OngoingAction(_)).toList.sortBy(_.launchTime),
          timestamp = ts,
          snapshot = metricReportType.snapshotType match {
            case MetricSnapshotType.Full =>
              MetricSnapshot.full(metricRegistry, ss.serviceParams)
            case MetricSnapshotType.Regular =>
              MetricSnapshot.regular(metricFilter, metricRegistry, ss.serviceParams)
            case MetricSnapshotType.Delta =>
              MetricSnapshot.delta(oldLast, metricFilter, metricRegistry, ss.serviceParams)
          }
        ))
    } yield ()

  /** Reset Counters only
    */
  def metricsReset(cronExpr: Option[CronExpr]): F[Unit] =
    for {
      ss <- serviceStatus.get
      ts <- F.realTimeInstant.map(ss.serviceParams.toZonedDateTime)
      msg = cronExpr.flatMap { ce =>
        ce.next(ts).map { next =>
          MetricReset(
            resetType = MetricResetType.Scheduled(next),
            serviceStatus = ss,
            timestamp = ts,
            snapshot = MetricSnapshot.regular(MetricFilter.ALL, metricRegistry, ss.serviceParams)
          )
        }
      }.getOrElse(
        MetricReset(
          resetType = MetricResetType.Adhoc,
          serviceStatus = ss,
          timestamp = ts,
          snapshot = MetricSnapshot.full(metricRegistry, ss.serviceParams)
        ))
      _ <- channel.send(msg)
      _ <- lastCounters.set(MetricSnapshot.LastCounters.empty)
    } yield metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount))

  // query
  val snapshotFull: F[MetricSnapshot] =
    serviceStatus.get.map(ss => MetricSnapshot.full(metricRegistry, ss.serviceParams))

  def snapshot(metricFilter: MetricFilter): F[MetricSnapshot] =
    serviceStatus.get.map(ss => MetricSnapshot.regular(metricFilter, metricRegistry, ss.serviceParams))

}
