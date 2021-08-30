package com.github.chenharryhua.nanjin.guard.event

import cats.effect.kernel.Temporal
import cats.effect.std.UUIDGen
import cats.implicits.{catsSyntaxApply, toFunctorOps}
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Importance, ServiceParams}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import fs2.concurrent.Channel
import io.circe.Json
import retry.RetryDetails
import retry.RetryDetails.WillDelayAndRetry

import java.time.{Duration, ZonedDateTime}
import scala.compat.java8.DurationConverters.FiniteDurationops
import scala.concurrent.duration.FiniteDuration

final private[guard] class EventPublisher[F[_]: UUIDGen](
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  serviceInfo: ServiceInfo,
  serviceParams: ServiceParams)(implicit F: Temporal[F]) {

  // service level
  private val metricsReportMRName: String = "01.health.check"
  private val serviceStartMRName: String  = "02.service.start"
  private val serviceStopMRName: String   = "03.service.stop"
  private val servicePanicMRName: String  = "04.service.`panic`"

  // action level
  private def passThroughMRName(params: ActionParams): String = s"10.pass.through.[${params.actionName}]"
  private def counterMRName(params: ActionParams): String     = s"11.counter.[${params.actionName}]"
  private def actionFailMRName(params: ActionParams): String  = s"12.action.[${params.actionName}].`fail`"
  private def actionStartMRName(params: ActionParams): String = s"12.action.[${params.actionName}].count"
  private def actionRetryMRName(params: ActionParams): String = s"12.action.[${params.actionName}].retry"
  private def actionSuccMRName(params: ActionParams): String  = s"12.action.[${params.actionName}].succd"

  private val realZonedDateTime: F[ZonedDateTime] = F.realTimeInstant.map(_.atZone(serviceParams.taskParams.zoneId))

  /** services
    */

  val serviceReStarted: F[Unit] =
    realZonedDateTime.flatMap(ts =>
      channel
        .send(ServiceStarted(ts, serviceInfo, serviceParams))
        .map(_ => metricRegistry.counter(serviceStartMRName).inc()))

  def servicePanic(retryDetails: RetryDetails, ex: Throwable): F[Unit] =
    realZonedDateTime
      .flatMap(ts => channel.send(ServicePanic(ts, serviceInfo, serviceParams, retryDetails, NJError(ex))))
      .map(_ => metricRegistry.counter(servicePanicMRName).inc())

  val serviceStopped: F[Unit] =
    realZonedDateTime
      .flatMap(ts => channel.send(ServiceStopped(ts, serviceInfo, serviceParams)))
      .map(_ => metricRegistry.counter(serviceStopMRName).inc())

  def metricsReport(index: Long, dur: FiniteDuration): F[Unit] =
    realZonedDateTime.flatMap { ts =>
      channel
        .send(
          MetricsReport(
            index = index,
            timestamp = ts,
            serviceInfo = serviceInfo,
            serviceParams = serviceParams,
            prev = Some(ts.minus(dur.toJava)),
            next = Some(ts.plus(dur.toJava)),
            metrics = MetricRegistryWrapper(
              registry = Some(metricRegistry),
              rateTimeUnit = serviceParams.metricsRateTimeUnit,
              durationTimeUnit = serviceParams.metricsDurationTimeUnit,
              zoneId = serviceParams.taskParams.zoneId
            )
          ))
        .map(_ => metricRegistry.counter(metricsReportMRName).inc())
    }

  def metricsReport(index: Long, cronExpr: CronExpr): F[Unit] =
    realZonedDateTime.flatMap(ts =>
      channel
        .send(
          MetricsReport(
            index = index,
            timestamp = ts,
            serviceInfo = serviceInfo,
            serviceParams = serviceParams,
            prev = cronExpr.prev(ts),
            next = cronExpr.next(ts),
            metrics = MetricRegistryWrapper(
              registry = Some(metricRegistry),
              rateTimeUnit = serviceParams.metricsRateTimeUnit,
              durationTimeUnit = serviceParams.metricsDurationTimeUnit,
              zoneId = serviceParams.taskParams.zoneId
            )
          ))
        .map(_ => metricRegistry.counter(metricsReportMRName).inc()))

  /** actions
    */

  def actionStart(actionParams: ActionParams): F[ActionInfo] =
    for {
      uuid <- UUIDGen.randomUUID
      ts <- realZonedDateTime
      actionInfo = ActionInfo(uuid, ts)
      _ <- actionParams.importance match {
        case Importance.High =>
          channel
            .send(ActionStart(actionParams, actionInfo, ts))
            .map(_ => metricRegistry.counter(actionStartMRName(actionParams)).inc())
        case Importance.Medium => F.pure(metricRegistry.counter(actionStartMRName(actionParams)).inc())
        case Importance.Low    => F.unit
      }
    } yield actionInfo

  private def timing(name: String, actionInfo: ActionInfo, timestamp: ZonedDateTime): F[Unit] =
    F.pure(metricRegistry.timer(name).update(Duration.between(actionInfo.launchTime, timestamp)))

  def actionSucced(actionInfo: ActionInfo, actionParams: ActionParams, numRetries: Int, notes: Notes): F[Unit] =
    actionParams.importance match {
      case Importance.High =>
        realZonedDateTime.flatMap(ts =>
          channel
            .send(
              ActionSucced(
                actionInfo = actionInfo,
                timestamp = ts,
                actionParams = actionParams,
                numRetries = numRetries,
                notes = notes))
            .flatMap(_ => timing(actionSuccMRName(actionParams), actionInfo, ts)))
      case Importance.Medium => realZonedDateTime.flatMap(ts => timing(actionSuccMRName(actionParams), actionInfo, ts))
      case Importance.Low    => F.unit
    }

  def quasiSucced(
    actionInfo: ActionInfo,
    actionParams: ActionParams,
    runMode: RunMode,
    numSucc: Long,
    succNotes: Notes,
    failNotes: Notes,
    errors: List[NJError]
  ): F[Unit] =
    actionParams.importance match {
      case Importance.High =>
        realZonedDateTime.flatMap(ts =>
          channel
            .send(
              ActionQuasiSucced(
                actionInfo = actionInfo,
                timestamp = ts,
                actionParams = actionParams,
                runMode = runMode,
                numSucc = numSucc,
                succNotes = succNotes,
                failNotes = failNotes,
                errors = errors
              ))
            .flatMap(_ => timing(actionSuccMRName(actionParams), actionInfo, ts)))
      case Importance.Medium => realZonedDateTime.flatMap(ts => timing(actionSuccMRName(actionParams), actionInfo, ts))
      case Importance.Low    => F.unit
    }

  def actionRetrying(
    actionInfo: ActionInfo,
    actionParams: ActionParams,
    willDelayAndRetry: WillDelayAndRetry,
    ex: Throwable): F[Unit] =
    realZonedDateTime.flatMap(ts =>
      channel
        .send(
          ActionRetrying(
            actionInfo = actionInfo,
            timestamp = ts,
            actionParams = actionParams,
            willDelayAndRetry = willDelayAndRetry,
            error = NJError(ex)))
        .flatMap(_ =>
          actionParams.importance match {
            case Importance.High | Importance.Medium => timing(actionRetryMRName(actionParams), actionInfo, ts)
            case Importance.Low                      => F.unit
          }))

  def actionFailed(
    actionInfo: ActionInfo,
    actionParams: ActionParams,
    numRetries: Int,
    notes: Notes,
    ex: Throwable
  ): F[Unit] =
    realZonedDateTime.flatMap(ts =>
      channel
        .send(
          ActionFailed(
            actionInfo = actionInfo,
            timestamp = ts,
            actionParams = actionParams,
            numRetries = numRetries,
            notes = notes,
            error = NJError(ex)))
        .flatMap(_ =>
          actionParams.importance match {
            case Importance.High | Importance.Medium => timing(actionFailMRName(actionParams), actionInfo, ts)
            case Importance.Low                      => F.unit
          }))

  def passThrough(actionParams: ActionParams, json: Json): F[Unit] =
    realZonedDateTime.flatMap(ts =>
      channel
        .send(PassThrough(ts, actionParams, json))
        .map(_ => metricRegistry.counter(passThroughMRName(actionParams)).inc()))

  def count(actionParams: ActionParams, num: Long): F[Unit] =
    F.pure(metricRegistry.counter(counterMRName(actionParams)).inc(num))
}
