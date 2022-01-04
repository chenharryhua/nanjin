package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Ref, Temporal}
import cats.effect.std.UUIDGen
import cats.implicits.{catsSyntaxApply, toFunctorOps}
import cats.syntax.all.*
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import fs2.concurrent.Channel
import org.apache.commons.lang3.exception.ExceptionUtils
import retry.RetryDetails

import scala.jdk.CollectionConverters.CollectionHasAsScala

final class ServiceEventPublisher[F[_]: UUIDGen](
  serviceParams: ServiceParams,
  serviceStatus: Ref[F, ServiceStatus],
  metricRegistry: MetricRegistry,
  ongoings: Ref[F, Set[ActionInfo]],
  lastCountersRef: Ref[F, MetricSnapshot.LastCounters],
  channel: Channel[F, NJEvent])(implicit F: Temporal[F]) {

  /** services
    */

  def serviceReStart: F[Unit] =
    for {
      ts <- realZonedDateTime(serviceParams.taskParams.zoneId)
      ss <- serviceStatus.updateAndGet(_.goUp(ts))
      _ <- channel.send(ServiceStart(ss, ts, serviceParams))
      _ <- ongoings.set(Set.empty)
    } yield ()

  def servicePanic(retryDetails: RetryDetails, ex: Throwable): F[Unit] =
    for {
      ts <- realZonedDateTime(serviceParams.taskParams.zoneId)
      ss <- serviceStatus.updateAndGet(_.goDown(ts, retryDetails.upcomingDelay, ExceptionUtils.getMessage(ex)))
      uuid <- UUIDGen.randomUUID[F]
      _ <- channel.send(ServicePanic(ss, ts, retryDetails, serviceParams, NJError(uuid, ex)))
    } yield ()

  def serviceStop: F[Unit] =
    for {
      ts <- realZonedDateTime(serviceParams.taskParams.zoneId)
      ss <- serviceStatus.updateAndGet(_.goDown(ts, None, cause = "service was stopped"))
      _ <- channel.send(
        ServiceStop(
          timestamp = ts,
          serviceStatus = ss,
          serviceParams = serviceParams,
          snapshot = MetricSnapshot.full(metricRegistry, serviceParams)
        ))
    } yield ()

  def metricsReport(metricFilter: MetricFilter, metricReportType: MetricReportType): F[Unit] =
    for {
      ts <- realZonedDateTime(serviceParams.taskParams.zoneId)
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
      ts <- realZonedDateTime(serviceParams.taskParams.zoneId)
      ss <- serviceStatus.get
      msg = cronExpr.flatMap { ce =>
        ce.next(ts).map { next =>
          MetricsReset(
            resetType = MetricResetType.Scheduled(next),
            serviceStatus = ss,
            timestamp = ts,
            serviceParams = serviceParams,
            snapshot = MetricSnapshot.full(metricRegistry, serviceParams)
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
