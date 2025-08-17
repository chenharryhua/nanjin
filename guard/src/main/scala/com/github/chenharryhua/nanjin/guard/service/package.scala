package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Unique.Token
import cats.effect.kernel.{Clock, Sync}
import cats.implicits.{catsSyntaxApplyOps, catsSyntaxTuple2Semigroupal, toFlatMapOps, toFunctorOps}
import cats.{Applicative, Hash, Monad}
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
        token = Hash[Token].hash(token),
        level = level,
        error = error,
        message = Encoder[S].apply(msg))
    }

  private[service] def metricReport[F[_]](
    channel: Channel[F, Event],
    eventLogger: EventLogger[F],
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    index: MetricIndex)(implicit F: Sync[F]): F[MetricReport] =
    for {
      (took, snapshot) <- MetricSnapshot.timed(metricRegistry)
      mr = MetricReport(index, serviceParams, snapshot, took)
      _ <- channel.send(mr)
      _ <- index match {
        case MetricIndex.Adhoc(_)       => eventLogger.metric_report(mr)
        case MetricIndex.Periodic(tick) =>
          if (tick.index % serviceParams.servicePolicies.metricReport.logRatio == 0)
            eventLogger.metric_report(mr)
          else F.unit
      }
    } yield mr

  private[service] def metricReset[F[_]: Sync](
    channel: Channel[F, Event],
    eventLogger: EventLogger[F],
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    index: MetricIndex): F[Unit] =
    for {
      (took, snapshot) <- MetricSnapshot.timed(metricRegistry)
      ms = MetricReset(index, serviceParams, snapshot, took)
      _ <- channel.send(ms)
      _ <- eventLogger.metric_reset(ms)
    } yield metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount))

  private[service] def serviceReStart[F[_]: Applicative](
    channel: Channel[F, Event],
    eventLogger: EventLogger[F],
    serviceParams: ServiceParams,
    tick: Tick): F[Unit] = {
    val event = ServiceStart(serviceParams, tick)
    eventLogger.service_start(event) <* channel.send(event)
  }

  private[service] def servicePanic[F[_]: Applicative](
    channel: Channel[F, Event],
    eventLogger: EventLogger[F],
    serviceParams: ServiceParams,
    tick: Tick,
    error: Error): F[ServicePanic] = {
    val panic: ServicePanic = ServicePanic(serviceParams, tick, error)
    eventLogger.service_panic(panic) *> channel.send(panic).as(panic)
  }

  private[service] def serviceStop[F[_]: Clock: Monad](
    channel: Channel[F, Event],
    eventLogger: EventLogger[F],
    serviceParams: ServiceParams,
    cause: ServiceStopCause): F[Unit] =
    serviceParams.zonedNow.flatMap { now =>
      val event = ServiceStop(serviceParams, now, cause)
      eventLogger.service_stop(event) <* channel.closeWithElement(event)
    }
}
