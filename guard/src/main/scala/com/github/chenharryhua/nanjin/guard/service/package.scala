package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.{Clock, Sync}
import cats.implicits.{catsSyntaxTuple2Semigroupal, toFlatMapOps, toFunctorOps}
import cats.{Functor, Monad}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.{
  MetricReport,
  MetricReset,
  ServiceMessage,
  ServicePanic,
  ServiceStart,
  ServiceStop
}
import com.github.chenharryhua.nanjin.guard.event.{
  Error,
  Event,
  MetricIndex,
  MetricSnapshot,
  ServiceStopCause
}
import fs2.concurrent.Channel
import io.circe.Encoder

import scala.jdk.CollectionConverters.CollectionHasAsScala

package object service {

  private[service] def toServiceMessage[F[_], S: Encoder](
    serviceParams: ServiceParams,
    msg: S,
    level: AlarmLevel,
    error: Option[Error])(implicit F: Sync[F]): F[ServiceMessage] =
    (F.unique, serviceParams.zonedNow).mapN { case (token, ts) =>
      ServiceMessage(
        serviceParams = serviceParams,
        timestamp = ts,
        token = token.hashCode(),
        level = level,
        error = error,
        message = Encoder[S].apply(msg))
    }

  private[service] def metricReport[F[_]: Sync](
    channel: Channel[F, Event],
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    index: MetricIndex): F[MetricReport] =
    for {
      (took, snapshot) <- MetricSnapshot.timed(metricRegistry)
      mr = MetricReport(index, serviceParams, snapshot, took)
      _ <- channel.send(mr)
    } yield mr

  private[service] def metricReset[F[_]: Sync](
    channel: Channel[F, Event],
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    index: MetricIndex): F[Unit] =
    for {
      (took, snapshot) <- MetricSnapshot.timed(metricRegistry)
      mr = MetricReset(index, serviceParams, snapshot, took)
      _ <- channel.send(mr)
    } yield metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount))

  private[service] def serviceReStart[F[_]: Functor](
    channel: Channel[F, Event],
    serviceParams: ServiceParams,
    tick: Tick): F[Unit] =
    channel.send(ServiceStart(serviceParams, tick)).void

  private[service] def servicePanic[F[_]: Functor](
    channel: Channel[F, Event],
    serviceParams: ServiceParams,
    tick: Tick,
    error: Error): F[ServicePanic] = {
    val panic: ServicePanic = ServicePanic(serviceParams, tick, error)
    channel.send(panic).as(panic)
  }

  private[service] def serviceStop[F[_]: Clock: Monad](
    channel: Channel[F, Event],
    serviceParams: ServiceParams,
    cause: ServiceStopCause): F[Unit] =
    serviceParams.zonedNow.flatMap { now =>
      channel.closeWithElement(ServiceStop(serviceParams, now, cause)).void
    }
}
