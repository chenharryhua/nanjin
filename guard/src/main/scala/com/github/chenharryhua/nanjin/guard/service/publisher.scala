package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Clock, Sync}
import cats.syntax.all.*
import cats.{Functor, Monad}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.Event.{
  MetricReport,
  MetricReset,
  ServicePanic,
  ServiceStart,
  ServiceStop
}
import fs2.concurrent.Channel

import scala.jdk.CollectionConverters.CollectionHasAsScala

private object publisher {
  def metricReport[F[_]: Sync](
    channel: Channel[F, Event],
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    index: MetricIndex): F[MetricReport] =
    for {
      (took, ss) <- MetricSnapshot.timed(metricRegistry)
      mr = MetricReport(index = index, serviceParams = serviceParams, snapshot = ss, took = took)
      _ <- channel.send(mr)
    } yield mr

  def metricReset[F[_]: Sync](
    channel: Channel[F, Event],
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    index: MetricIndex): F[Unit] =
    for {
      (took, ss) <- MetricSnapshot.timed(metricRegistry)
      mr = MetricReset(index = index, serviceParams = serviceParams, snapshot = ss, took = took)
      _ <- channel.send(mr)
    } yield metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount))

  def serviceReStart[F[_]: Functor](
    channel: Channel[F, Event],
    serviceParams: ServiceParams,
    tick: Tick): F[Unit] =
    channel.send(ServiceStart(serviceParams, tick)).void

  def servicePanic[F[_]: Functor](
    channel: Channel[F, Event],
    serviceParams: ServiceParams,
    tick: Tick,
    error: Error): F[ServicePanic] = {
    val panic: ServicePanic = ServicePanic(serviceParams, tick, error)
    channel.send(panic).as(panic)
  }

  def serviceStop[F[_]: Clock: Monad](
    channel: Channel[F, Event],
    serviceParams: ServiceParams,
    cause: ServiceStopCause): F[Unit] =
    serviceParams.zonedNow.flatMap { now =>
      channel.closeWithElement(ServiceStop(serviceParams, now, cause)).void
    }

}
