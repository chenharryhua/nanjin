package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Async, Ref, Temporal, Unique}
import cats.effect.kernel.Resource.ExitCase
import cats.syntax.all.*
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.action.ActionException
import com.github.chenharryhua.nanjin.guard.config.{MetricSnapshotType, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{
  MetricReport,
  MetricReset,
  RootSpanFinish,
  RootSpanStart,
  ServicePanic,
  ServiceStart,
  ServiceStop
}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import fs2.concurrent.Channel

import java.time.ZonedDateTime
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.DurationConverters.ScalaDurationOps

private object publisher {
  def metricReport[F[_]](
    channel: Channel[F, NJEvent],
    serviceStatus: Ref[F, ServiceStatus],
    metricRegistry: MetricRegistry,
    metricFilter: MetricFilter,
    metricReportType: MetricReportType)(implicit F: Async[F]): F[Unit] =
    for {
      ss <- serviceStatus.getAndUpdate(_.updateLastCounters(MetricSnapshot.LastCounters(metricRegistry)))
      ts <- F.realTimeInstant.map(ss.serviceParams.toZonedDateTime)
      _ <- channel.send(
        MetricReport(
          serviceParams = ss.serviceParams,
          reportType = metricReportType,
          timestamp = ts,
          snapshot = metricReportType.snapshotType match {
            case MetricSnapshotType.Full =>
              MetricSnapshot.full(metricRegistry, ss.serviceParams)
            case MetricSnapshotType.Regular =>
              MetricSnapshot.regular(metricFilter, metricRegistry, ss.serviceParams)
            case MetricSnapshotType.Delta =>
              MetricSnapshot.delta(ss.lastCounters, metricFilter, metricRegistry, ss.serviceParams)
          },
          restartTime = ss.upcomingRestartTime
        ))
    } yield ()

  def metricReset[F[_]](
    channel: Channel[F, NJEvent],
    serviceStatus: Ref[F, ServiceStatus],
    metricRegistry: MetricRegistry,
    cronExpr: Option[CronExpr])(implicit F: Async[F]): F[Unit] =
    for {
      ss <- serviceStatus.getAndUpdate(_.updateLastCounters(MetricSnapshot.LastCounters.empty))
      ts <- F.realTimeInstant.map(ss.serviceParams.toZonedDateTime)
      msg = cronExpr.flatMap { ce =>
        ce.next(ts).map { next =>
          MetricReset(
            resetType = MetricResetType.Scheduled(next),
            serviceParams = ss.serviceParams,
            timestamp = ts,
            snapshot = MetricSnapshot.regular(MetricFilter.ALL, metricRegistry, ss.serviceParams)
          )
        }
      }.getOrElse(
        MetricReset(
          resetType = MetricResetType.Adhoc,
          serviceParams = ss.serviceParams,
          timestamp = ts,
          snapshot = MetricSnapshot.full(metricRegistry, ss.serviceParams)
        ))
      _ <- channel.send(msg)
    } yield metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount))

  def serviceReStart[F[_]](channel: Channel[F, NJEvent], serviceStatus: Ref[F, ServiceStatus])(implicit
    F: Temporal[F]): F[Unit] =
    for {
      sp <- serviceStatus.get.map(_.serviceParams)
      ts <- F.realTimeInstant.map(sp.toZonedDateTime)
      _ <- serviceStatus.update(_.goUp(ts))
      _ <- channel.send(ServiceStart(sp, ts))
    } yield ()

  def servicePanic[F[_]](
    channel: Channel[F, NJEvent],
    serviceStatus: Ref[F, ServiceStatus],
    delay: FiniteDuration,
    ex: Throwable)(implicit F: Temporal[F]): F[Unit] =
    for {
      ts <- F.realTimeInstant
      sp <- serviceStatus.get.map(_.serviceParams)
      now  = sp.toZonedDateTime(ts)
      next = sp.toZonedDateTime(ts.plus(delay.toJava))
      err  = NJError(ex)
      _ <- serviceStatus.update(_.goPanic(now, next, err))
      _ <- channel.send(ServicePanic(serviceParams = sp, timestamp = now, restartTime = next, error = err))
    } yield ()

  def serviceStop[F[_]](
    channel: Channel[F, NJEvent],
    serviceStatus: Ref[F, ServiceStatus],
    cause: ServiceStopCause)(implicit F: Temporal[F]): F[Unit] =
    for {
      sp <- serviceStatus.get.map(_.serviceParams)
      ts <- F.realTimeInstant.map(sp.toZonedDateTime)
      _ <- channel.send(ServiceStop(timestamp = ts, serviceParams = sp, cause = cause))
    } yield ()

  def rootSpanStart[F[_]](channel: Channel[F, NJEvent], serviceParams: ServiceParams, rootSpanName: String)(
    implicit F: Temporal[F]): F[(ZonedDateTime, Int)] =
    for {
      tid <- Unique[F].unique.map(_.hash)
      ts <- F.realTimeInstant.map(serviceParams.toZonedDateTime)
      _ <- channel.send(
        RootSpanStart(
          serviceParams = serviceParams,
          timestamp = ts,
          rootSpanName = rootSpanName,
          internalTraceId = tid))
    } yield (ts, tid)

  def rootSpanFinish[F[_]](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    rootSpanName: String,
    internalTraceId: Int,
    launchTime: ZonedDateTime,
    exitCase: ExitCase)(implicit F: Temporal[F]): F[Unit] =
    for {
      ts <- F.realTimeInstant.map(serviceParams.toZonedDateTime)
      _ <- channel.send(
        RootSpanFinish(
          serviceParams = serviceParams,
          timestamp = ts,
          rootSpanName = rootSpanName,
          internalTraceId = internalTraceId,
          launchTime = launchTime,
          result = exitCase match {
            case ExitCase.Succeeded  => None
            case ExitCase.Errored(e) => Some(NJError(e))
            case ExitCase.Canceled   => Some(NJError(ActionException.ActionCanceled))
          }
        ))
    } yield ()
}
