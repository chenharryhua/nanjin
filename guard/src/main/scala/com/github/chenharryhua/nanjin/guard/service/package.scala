package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.{Clock, Sync}
import cats.syntax.apply.{catsSyntaxApplyOps, catsSyntaxTuple5Semigroupal}
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.option.{catsSyntaxOptionId, none}
import cats.{Applicative, Functor, Monad, Semigroupal}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.{
  MetricsSnapshot,
  ServicePanic,
  ServiceStart,
  ServiceStop
}
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.{Index, Kind}
import com.github.chenharryhua.nanjin.guard.event.{
  Event,
  ScrapeMode,
  Snapshot,
  StackTrace,
  StopReason,
  Timestamp,
  Took
}
import com.github.chenharryhua.nanjin.guard.logging.LogSink
import fs2.concurrent.Channel
import org.typelevel.log4cats.SelfAwareLogger

import scala.jdk.CollectionConverters.CollectionHasAsScala

package object service {

  private[service] def get_alarm_level[F[_]: Functor: Semigroupal](
    log: SelfAwareLogger[F]): F[Option[AlarmLevel]] =
    (log.isTraceEnabled, log.isDebugEnabled, log.isInfoEnabled, log.isWarnEnabled, log.isErrorEnabled).mapN {
      case (trace, debug, info, warn, error) =>
        if (trace) AlarmLevel.Debug.some
        else if (debug) AlarmLevel.Debug.some
        else if (info) AlarmLevel.Info.some
        else if (warn) AlarmLevel.Warn.some
        else if (error) AlarmLevel.Error.some
        else none[AlarmLevel]
    }

  private[service] def create_metrics_report[F[_]: Sync](
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    index: Index,
    mode: ScrapeMode): F[MetricsSnapshot] =
    Snapshot.timed[F](metricRegistry, mode).map { case (took, snapshot) =>
      MetricsSnapshot(
        index,
        serviceParams,
        snapshot,
        Kind.Report(serviceParams.servicePolicies.metricsReport),
        Took(took))
    }

  private[service] def publish_metrics_report[F[_]](
    serviceParams: ServiceParams,
    channel: Channel[F, Event],
    logSink: LogSink[F],
    metricRegistry: MetricRegistry,
    index: Index)(implicit F: Sync[F]): F[MetricsSnapshot] =
    for {
      mr <- create_metrics_report(serviceParams, metricRegistry, index, ScrapeMode.Full)
      _ <- logSink.write(mr)
      _ <- channel.send(mr)
    } yield mr

  private[service] def publish_metrics_reset[F[_]: Sync](
    serviceParams: ServiceParams,
    channel: Channel[F, Event],
    logSink: LogSink[F],
    metricRegistry: MetricRegistry,
    index: Index): F[MetricsSnapshot] =
    for {
      (took, snapshot) <- Snapshot.timed[F](metricRegistry, ScrapeMode.Full)
      ms = MetricsSnapshot(
        index,
        serviceParams,
        snapshot,
        Kind.Reset(serviceParams.servicePolicies.metricsReset),
        Took(took))
      _ <- logSink.write(ms)
      _ <- channel.send(ms)
    } yield {
      metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount))
      ms
    }

  private[service] def publish_service_start[F[_]: Applicative](
    serviceParams: ServiceParams,
    channel: Channel[F, Event],
    logSink: LogSink[F],
    tick: Tick): F[Unit] = {
    val event = ServiceStart(serviceParams, tick)
    logSink.write(event) <* channel.send(event)
  }

  private[service] def publish_service_panic[F[_]: Applicative](
    serviceParams: ServiceParams,
    channel: Channel[F, Event],
    logSink: LogSink[F],
    tick: Tick,
    stackTrace: StackTrace): F[ServicePanic] = {
    val panic: ServicePanic = ServicePanic(serviceParams, tick, stackTrace)
    logSink.write(panic) *> channel.send(panic).as(panic)
  }

  private[service] def publish_service_stop[F[_]: Clock: Monad](
    serviceParams: ServiceParams,
    channel: Channel[F, Event],
    logSink: LogSink[F],
    cause: StopReason): F[Unit] =
    for {
      now <- serviceParams.zonedNow
      event = ServiceStop(serviceParams, Timestamp(now), cause)
      _ <- logSink.write(event)
      _ <- channel.closeWithElement(event)
    } yield ()
}
