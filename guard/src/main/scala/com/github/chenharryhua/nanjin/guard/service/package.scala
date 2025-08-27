package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Unique.Token
import cats.effect.kernel.{Clock, Sync}
import cats.implicits.{
  catsSyntaxApplyOps,
  catsSyntaxEq,
  catsSyntaxOptionId,
  catsSyntaxTuple2Semigroupal,
  catsSyntaxTuple5Semigroupal,
  toFlatMapOps,
  toFunctorOps,
  toShow
}
import cats.syntax.all.none
import cats.{Applicative, Functor, Hash, Monad, Semigroupal}
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
import com.github.chenharryhua.nanjin.guard.translator.durationFormatter
import fs2.concurrent.Channel
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
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

  private[service] def interpret_service_params(sp: ServiceParams): Json =
    Json.obj(
      "home_page" -> sp.homePage.asJson,
      "policies" -> Json.obj(
        "restart" -> sp.servicePolicies.restart.policy.show.asJson,
        "threshold" -> sp.servicePolicies.restart.threshold.map(durationFormatter.format).asJson,
        "metric_report" -> sp.servicePolicies.metricReport.policy.show.asJson,
        "metric_reset" -> sp.servicePolicies.metricReset.show.asJson
      ),
      "history_capacity" -> Json.obj(
        "metric" -> sp.historyCapacity.metric.asJson,
        "panic" -> sp.historyCapacity.panic.asJson,
        "error" -> sp.historyCapacity.error.asJson
      ),
      "log_format" -> sp.logFormat.asJson,
      "brief" -> sp.brief
    )

  private[service] def serviceMessage[F[_], S: Encoder](
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
    metricRegistry: MetricRegistry,
    index: MetricIndex)(implicit F: Sync[F]): F[MetricReport] =
    for {
      (took, snapshot) <- MetricSnapshot.timed(metricRegistry)
      mr = MetricReport(index, eventLogger.serviceParams, snapshot, took)
      _ <- index match {
        case MetricIndex.Adhoc(_)       => eventLogger.metric_report(mr)
        case MetricIndex.Periodic(tick) =>
          if (tick.index % eventLogger.serviceParams.servicePolicies.metricReport.logRatio === 0)
            eventLogger.metric_report(mr)
          else F.unit
      }
      _ <- channel.send(mr)
    } yield mr

  private[service] def metricReset[F[_]: Sync](
    channel: Channel[F, Event],
    eventLogger: EventLogger[F],
    metricRegistry: MetricRegistry,
    index: MetricIndex): F[Unit] =
    for {
      (took, snapshot) <- MetricSnapshot.timed(metricRegistry)
      ms = MetricReset(index, eventLogger.serviceParams, snapshot, took)
      _ <- eventLogger.metric_reset(ms)
      _ <- channel.send(ms)
    } yield metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount))

  private[service] def serviceStart[F[_]: Applicative](
    channel: Channel[F, Event],
    eventLogger: EventLogger[F],
    tick: Tick): F[Unit] = {
    val event = ServiceStart(eventLogger.serviceParams, tick)
    eventLogger.service_start(event) <* channel.send(event)
  }

  private[service] def servicePanic[F[_]: Applicative](
    channel: Channel[F, Event],
    eventLogger: EventLogger[F],
    tick: Tick,
    error: Error): F[ServicePanic] = {
    val panic: ServicePanic = ServicePanic(eventLogger.serviceParams, tick, error)
    eventLogger.service_panic(panic) *> channel.send(panic).as(panic)
  }

  private[service] def serviceStop[F[_]: Clock: Monad](
    channel: Channel[F, Event],
    eventLogger: EventLogger[F],
    cause: ServiceStopCause): F[Unit] =
    for {
      now <- eventLogger.serviceParams.zonedNow
      event = ServiceStop(eventLogger.serviceParams, now, cause)
      _ <- eventLogger.service_stop(event)
      _ <- channel.closeWithElement(event)
    } yield ()
}
