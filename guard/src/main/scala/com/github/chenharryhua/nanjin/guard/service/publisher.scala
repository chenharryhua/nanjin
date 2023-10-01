package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Clock
import cats.syntax.all.*
import cats.{Functor, Monad}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.Tick
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

import java.time.Instant
import scala.jdk.CollectionConverters.CollectionHasAsScala

private object publisher {
  def metricReport[F[_]](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    index: MetricIndex,
    ts: Instant)(implicit F: Functor[F]): F[Unit] = {
    val msg = MetricReport(
      index = index,
      serviceParams = serviceParams,
      timestamp = serviceParams.toZonedDateTime(ts),
      snapshot = MetricSnapshot(metricRegistry))

    channel.send(msg).void
  }

  def metricReset[F[_]](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    index: MetricIndex,
    ts: Instant)(implicit F: Functor[F]): F[Unit] = {
    val msg = MetricReset(
      index = index,
      serviceParams = serviceParams,
      timestamp = serviceParams.toZonedDateTime(ts),
      snapshot = MetricSnapshot(metricRegistry))

    channel.send(msg).map(_ => metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount)))
  }

  def serviceReStart[F[_]](channel: Channel[F, NJEvent], serviceParams: ServiceParams, tick: Tick)(implicit
    F: Functor[F]): F[Unit] =
    channel.send(ServiceStart(serviceParams, tick)).void

  def servicePanic[F[_]](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    tick: Tick,
    ex: Throwable)(implicit F: Functor[F]): F[Unit] =
    channel.send(ServicePanic(serviceParams, NJError(ex), tick)).void

  def serviceStop[F[_]: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    cause: ServiceStopCause)(implicit F: Monad[F]): F[Unit] =
    serviceParams.zonedNow.flatMap { now =>
      channel.closeWithElement(ServiceStop(serviceParams, now, cause)).void
    }

}
