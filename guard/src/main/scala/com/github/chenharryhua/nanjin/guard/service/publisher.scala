package com.github.chenharryhua.nanjin.guard.service

import cats.Monad
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

import java.time.ZonedDateTime
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.DurationConverters.ScalaDurationOps

private object publisher {
  def metricReport[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    index: MetricIndex): F[Unit] =
    serviceParams.zonedNow
      .flatMap(ts =>
        channel.send(
          MetricReport(
            index = index,
            serviceParams = serviceParams,
            timestamp = ts,
            snapshot = MetricSnapshot(metricRegistry))))
      .void

  def metricReset[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    index: MetricIndex): F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- channel.send(
        MetricReset(
          index = index,
          serviceParams = serviceParams,
          timestamp = ts,
          snapshot = MetricSnapshot(metricRegistry)
        ))
    } yield metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount))

  def serviceReStart[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams): F[ZonedDateTime] =
    for {
      now <- serviceParams.zonedNow
      _ <- channel.send(ServiceStart(serviceParams, now))
    } yield now

  def servicePanic[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    delay: FiniteDuration,
    ex: Throwable): F[ZonedDateTime] =
    for {
      ts <- Clock[F].realTimeInstant
      now  = serviceParams.toZonedDateTime(ts)
      next = serviceParams.toZonedDateTime(ts.plus(delay.toJava))
      err  = NJError(ex)
      _ <- channel.send(
        ServicePanic(serviceParams = serviceParams, timestamp = now, restartTime = next, error = err))
    } yield now

  def serviceStop[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    cause: ServiceStopCause): F[Unit] =
    for {
      now <- serviceParams.zonedNow
      _ <- channel.send(ServiceStop(timestamp = now, serviceParams = serviceParams, cause = cause))
    } yield ()

}
