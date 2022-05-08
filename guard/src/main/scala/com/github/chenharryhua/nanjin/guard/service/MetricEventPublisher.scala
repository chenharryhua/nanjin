package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Async, Ref}
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
  serviceStatus: Ref[F, ServiceStatus])(implicit F: Async[F]) {

  def metricsReport(metricFilter: MetricFilter, metricReportType: MetricReportType): F[Unit] =
    for {
      ss <- serviceStatus.getAndUpdate(_.updateLastCounters(MetricSnapshot.LastCounters(metricRegistry)))
      ts <- F.realTimeInstant.map(ss.serviceParams.toZonedDateTime)
      _ <- channel.send(
        MetricReport(
          serviceParams = ss.serviceParams,
          reportType = metricReportType,
          ongoings = ss.ongoingActions.toList.sortBy(_.launchTime),
          timestamp = ts,
          snapshot = metricReportType.snapshotType match {
            case MetricSnapshotType.Full =>
              MetricSnapshot.full(metricRegistry, ss.serviceParams)
            case MetricSnapshotType.Regular =>
              MetricSnapshot.regular(metricFilter, metricRegistry, ss.serviceParams)
            case MetricSnapshotType.Delta =>
              MetricSnapshot.delta(ss.lastCounters, metricFilter, metricRegistry, ss.serviceParams)
          },
          upcommingRestart = ss.upcommingRestart,
          isUp = ss.isUp
        ))
    } yield ()

  /** Reset Counters only
    */
  def metricsReset(cronExpr: Option[CronExpr]): F[Unit] =
    for {
      ss <- serviceStatus.getAndUpdate(_.updateLastCounters(MetricSnapshot.LastCounters.empty))
      ts <- F.realTimeInstant.map(ss.serviceParams.toZonedDateTime)
      msg = cronExpr.flatMap { ce =>
        ce.next(ts).map { next =>
          MetricReset(
            resetType = MetricResetType.Scheduled(next),
            serviceParams = ss.serviceParams,
            timestamp = ts,
            snapshot = MetricSnapshot.regular(MetricFilter.ALL, metricRegistry, ss.serviceParams),
            isUp = ss.isUp
          )
        }
      }.getOrElse(MetricReset(
        resetType = MetricResetType.Adhoc,
        serviceParams = ss.serviceParams,
        timestamp = ts,
        snapshot = MetricSnapshot.full(metricRegistry, ss.serviceParams),
        isUp = ss.isUp
      ))
      _ <- channel.send(msg)
    } yield metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount))

  // query
  val snapshotFull: F[MetricSnapshot] =
    serviceStatus.get.map(ss => MetricSnapshot.full(metricRegistry, ss.serviceParams))

  def snapshot(metricFilter: MetricFilter): F[MetricSnapshot] =
    serviceStatus.get.map(ss => MetricSnapshot.regular(metricFilter, metricRegistry, ss.serviceParams))

}
