package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Clock, Ref}
import cats.syntax.all.*
import cats.Monad
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.MetricSnapshotType
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{
  MetricReport,
  MetricReset,
  ServicePanic,
  ServiceStart,
  ServiceStop
}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import fs2.concurrent.Channel

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.DurationConverters.ScalaDurationOps

private object publisher {
  def metricReport[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    serviceStatus: Ref[F, ServiceStatus],
    metricRegistry: MetricRegistry,
    metricFilter: MetricFilter,
    metricReportType: MetricReportType): F[Unit] =
    for {
      ss <- serviceStatus.getAndUpdate(_.updateLastCounters(MetricSnapshot.LastCounters(metricRegistry)))
      ts <- Clock[F].realTimeInstant.map(ss.serviceParams.toZonedDateTime)
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

  def metricReset[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    serviceStatus: Ref[F, ServiceStatus],
    metricRegistry: MetricRegistry,
    cronExpr: Option[CronExpr]): F[Unit] =
    for {
      ss <- serviceStatus.getAndUpdate(_.updateLastCounters(MetricSnapshot.LastCounters.empty))
      ts <- Clock[F].realTimeInstant.map(ss.serviceParams.toZonedDateTime)
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

  def serviceReStart[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    serviceStatus: Ref[F, ServiceStatus]): F[Unit] =
    for {
      sp <- serviceStatus.get.map(_.serviceParams)
      ts <- Clock[F].realTimeInstant.map(sp.toZonedDateTime)
      _ <- serviceStatus.update(_.goUp(ts))
      _ <- channel.send(ServiceStart(sp, ts))
    } yield ()

  def servicePanic[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    serviceStatus: Ref[F, ServiceStatus],
    delay: FiniteDuration,
    ex: Throwable): F[Unit] =
    for {
      ts <- Clock[F].realTimeInstant
      sp <- serviceStatus.get.map(_.serviceParams)
      now  = sp.toZonedDateTime(ts)
      next = sp.toZonedDateTime(ts.plus(delay.toJava))
      err  = NJError(ex)
      _ <- serviceStatus.update(_.goPanic(now, next, err))
      _ <- channel.send(ServicePanic(serviceParams = sp, timestamp = now, restartTime = next, error = err))
    } yield ()

  def serviceStop[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    serviceStatus: Ref[F, ServiceStatus],
    cause: ServiceStopCause): F[Unit] =
    for {
      sp <- serviceStatus.get.map(_.serviceParams)
      ts <- Clock[F].realTimeInstant.map(sp.toZonedDateTime)
      _ <- channel.send(ServiceStop(timestamp = ts, serviceParams = sp, cause = cause))
    } yield ()

}
