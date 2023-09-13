package com.github.chenharryhua.nanjin.guard.service

import cats.MonadError
import cats.effect.kernel.Clock
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{
  MetricReport,
  MetricReset,
  ServicePanic,
  ServiceStart,
  ServiceStop
}
import fs2.concurrent.Channel

import java.time.{Instant, ZonedDateTime}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.DurationConverters.ScalaDurationOps

private object publisher {
  def metricReport[F[_]](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    index: MetricIndex,
    ts: Instant)(implicit F: MonadError[F, Throwable]): F[Unit] =
    channel
      .send(
        MetricReport(
          index = index,
          serviceParams = serviceParams,
          timestamp = serviceParams.toZonedDateTime(ts),
          snapshot = MetricSnapshot(metricRegistry)))
      .void

  def metricReset[F[_]](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    index: MetricIndex,
    ts: Instant)(implicit F: MonadError[F, Throwable]): F[Unit] =
    channel
      .send(
        MetricReset(
          index = index,
          serviceParams = serviceParams,
          timestamp = serviceParams.toZonedDateTime(ts),
          snapshot = MetricSnapshot(metricRegistry)
        ))
      .map(_ => metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount)))

  def serviceReStart[F[_]: Clock](channel: Channel[F, NJEvent], serviceParams: ServiceParams)(implicit
    F: MonadError[F, Throwable]): F[ZonedDateTime] =
    for {
      now <- serviceParams.zonedNow
      _ <- channel
        .send(ServiceStart(serviceParams, now))
        .map(_.leftMap(_ => new Exception("service restart channel closed")))
        .rethrow
    } yield now

  def servicePanic[F[_]: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    delay: FiniteDuration,
    ex: Throwable)(implicit F: MonadError[F, Throwable]): F[ZonedDateTime] =
    for {
      ts <- Clock[F].realTimeInstant
      now  = serviceParams.toZonedDateTime(ts)
      next = serviceParams.toZonedDateTime(ts.plus(delay.toJava))
      err  = NJError(ex)
      _ <- channel
        .send(ServicePanic(serviceParams = serviceParams, timestamp = now, restartTime = next, error = err))
        .map(_.leftMap(_ => new Exception("service panic channel closed")))
        .rethrow
    } yield now

  def serviceStop[F[_]: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    cause: ServiceStopCause)(implicit F: MonadError[F, Throwable]): F[Unit] =
    serviceParams.zonedNow.flatMap(now =>
      channel
        .closeWithElement(ServiceStop(serviceParams = serviceParams, timestamp = now, cause = cause))
        .void)

}
