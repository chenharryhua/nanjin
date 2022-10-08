package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Clock
import cats.syntax.all.*
import cats.Monad
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
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

private object serviceEventPublisher {
  def metricReport[F[_]: Monad: Clock](
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    metricFilter: MetricFilter,
    metricReportType: MetricReportType): F[MetricReport] =
    serviceParams.zonedNow.map(ts =>
      MetricReport(
        serviceParams = serviceParams,
        reportType = metricReportType,
        timestamp = ts,
        snapshot = MetricSnapshot.regular(metricFilter, metricRegistry, serviceParams)
      ))

  def metricReset[F[_]: Monad: Clock](
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    cronExpr: Option[CronExpr]): F[MetricReset] =
    for {
      ts <- serviceParams.zonedNow
      evt = cronExpr.flatMap { ce =>
        ce.next(ts).map { next =>
          MetricReset(
            resetType = MetricResetType.Scheduled(next),
            serviceParams = serviceParams,
            timestamp = ts,
            snapshot = MetricSnapshot.full(metricRegistry, serviceParams)
          )
        }
      }.getOrElse(
        MetricReset(
          resetType = MetricResetType.Adhoc,
          serviceParams = serviceParams,
          timestamp = ts,
          snapshot = MetricSnapshot.full(metricRegistry, serviceParams)
        ))
    } yield {
      metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount))
      evt
    }

  def serviceReStart[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams): F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- channel.send(ServiceStart(serviceParams, ts))
    } yield ()

  def servicePanic[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    delay: FiniteDuration,
    ex: Throwable): F[Unit] =
    for {
      ts <- Clock[F].realTimeInstant
      now  = serviceParams.toZonedDateTime(ts)
      next = serviceParams.toZonedDateTime(ts.plus(delay.toJava))
      err  = NJError(ex)
      _ <- channel.send(
        ServicePanic(serviceParams = serviceParams, timestamp = now, restartTime = next, error = err))
    } yield ()

  def serviceStop[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    cause: ServiceStopCause): F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- channel.send(ServiceStop(timestamp = ts, serviceParams = serviceParams, cause = cause))
    } yield ()

}
