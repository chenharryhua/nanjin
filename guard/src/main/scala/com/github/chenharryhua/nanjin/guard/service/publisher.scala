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

import scala.jdk.CollectionConverters.CollectionHasAsScala

private object publisher {
  def metricReport[F[_]: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    index: MetricIndex,
    previous: MetricSnapshot,
    snapshot: MetricSnapshot)(implicit F: Monad[F]): F[MetricReport] =
    for {
      now <- serviceParams.zonedNow[F]
      mr = MetricReport(
        index = index,
        serviceParams = serviceParams,
        previous = previous,
        snapshot = snapshot,
        timestamp = now)
      _ <- channel.send(mr)
    } yield mr

  def metricReset[F[_]: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    index: MetricIndex)(implicit F: Monad[F]): F[Unit] =
    for {
      ss <- F.pure(MetricSnapshot(metricRegistry))
      now <- serviceParams.zonedNow[F]
      _ <- channel.send(MetricReset(index, serviceParams, ss, now))
    } yield metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount))

  def serviceReStart[F[_]](channel: Channel[F, NJEvent], serviceParams: ServiceParams, tick: Tick)(implicit
    F: Functor[F]): F[Unit] =
    channel.send(ServiceStart(serviceParams, tick)).void

  def servicePanic[F[_]](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    tick: Tick,
    error: NJError)(implicit F: Functor[F]): F[ServicePanic] = {
    val panic: ServicePanic = ServicePanic(serviceParams, tick, error)
    channel.send(panic).as(panic)
  }

  def serviceStop[F[_]: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    cause: ServiceStopCause)(implicit F: Monad[F]): F[Unit] =
    serviceParams.zonedNow.flatMap { now =>
      channel.closeWithElement(ServiceStop(serviceParams, now, cause)).void
    }

}
