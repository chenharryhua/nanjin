package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.{Clock, Sync}
import cats.syntax.apply.{catsSyntaxApplyOps, catsSyntaxTuple2Semigroupal, catsSyntaxTuple5Semigroupal}
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.option.{catsSyntaxOptionId, none}
import cats.syntax.show.toShow
import cats.{Applicative, Functor, Monad, Semigroupal}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, Domain, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.{
  MetricsReport,
  MetricsReset,
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
  ScrapeMode,
  ServiceStopCause
}
import fs2.concurrent.Channel
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.typelevel.log4cats.SelfAwareLogger

import scala.jdk.CollectionConverters.CollectionHasAsScala
import com.github.chenharryhua.nanjin.guard.event.Correlation

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

  private[service] def interpret_service_params(serviceParams: ServiceParams): Json =
    Json.obj(
      "task_name" -> serviceParams.taskName.asJson,
      "service_name" -> serviceParams.serviceName.asJson,
      "service_id" -> serviceParams.serviceId.asJson,
      "home_page" -> serviceParams.homePage.asJson,
      "host" -> Json.obj(
        "name" -> serviceParams.host.name.asJson,
        "port" -> serviceParams.host.port.asJson
      ),
      "service_policies" -> Json.obj(
        "restart" -> Json.obj(
          "policy" -> serviceParams.servicePolicies.restart.policy.show.asJson,
          "threshold" -> serviceParams.servicePolicies.restart.threshold.map(defaultFormatter.format).asJson
        ),
        "metrics_report" -> serviceParams.servicePolicies.metricsReport.show.asJson,
        "metrics_reset" -> serviceParams.servicePolicies.metricsReset.show.asJson
      ),
      "launch_time" -> serviceParams.launchTime.asJson,
      "log_format" -> serviceParams.logFormat.asJson,
      "history_capacity" -> Json.obj(
        "metrics_queue_length" -> serviceParams.historyCapacity.metric.asJson,
        "error_queue_length" -> serviceParams.historyCapacity.error.asJson,
        "panic_queue_length" -> serviceParams.historyCapacity.panic.asJson
      ),
      "nanjin" -> serviceParams.nanjin.asJson,
      "brief" -> serviceParams.brief
    )

  private[service] def create_service_message[F[_], S: Encoder](
    serviceParams: ServiceParams,
    domain: Domain,
    msg: S,
    level: AlarmLevel,
    error: Option[Error])(implicit F: Sync[F]): F[ServiceMessage] =
    (F.unique, serviceParams.zonedNow).mapN { case (token, ts) =>
      ServiceMessage(
        serviceParams = serviceParams,
        domain = domain,
        timestamp = ts,
        correlation = Correlation(token),
        level = level,
        error = error,
        message = Encoder[S].apply(msg))
    }

  private[service] def create_metrics_report[F[_]: Sync](
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    index: MetricIndex,
    mode: ScrapeMode): F[MetricsReport] =
    MetricSnapshot.timed[F](metricRegistry, mode).map { case (took, snapshot) =>
      MetricsReport(index, serviceParams, snapshot, took)
    }

  private[service] def publish_metrics_report[F[_]](
    channel: Channel[F, Event],
    eventLogger: EventLogger[F],
    metricRegistry: MetricRegistry,
    index: MetricIndex)(implicit F: Sync[F]): F[MetricsReport] =
    for {
      mr <- create_metrics_report(eventLogger.serviceParams, metricRegistry, index, ScrapeMode.Full)
      _ <- eventLogger.metrics_report(mr)
      _ <- channel.send(mr)
    } yield mr

  private[service] def publish_metrics_reset[F[_]: Sync](
    channel: Channel[F, Event],
    eventLogger: EventLogger[F],
    metricRegistry: MetricRegistry,
    index: MetricIndex): F[Unit] =
    for {
      (took, snapshot) <- MetricSnapshot.timed[F](metricRegistry, ScrapeMode.Full)
      ms = MetricsReset(index, eventLogger.serviceParams, snapshot, took)
      _ <- eventLogger.metrics_reset(ms)
      _ <- channel.send(ms)
    } yield metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount))

  private[service] def publish_service_start[F[_]: Applicative](
    channel: Channel[F, Event],
    eventLogger: EventLogger[F],
    tick: Tick): F[Unit] = {
    val event = ServiceStart(eventLogger.serviceParams, tick)
    eventLogger.service_start(event) <* channel.send(event)
  }

  private[service] def publish_service_panic[F[_]: Applicative](
    channel: Channel[F, Event],
    eventLogger: EventLogger[F],
    tick: Tick,
    error: Error): F[ServicePanic] = {
    val panic: ServicePanic = ServicePanic(eventLogger.serviceParams, tick, error)
    eventLogger.service_panic(panic) *> channel.send(panic).as(panic)
  }

  private[service] def publish_service_stop[F[_]: Clock: Monad](
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
