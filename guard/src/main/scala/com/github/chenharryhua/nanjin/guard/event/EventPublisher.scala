package com.github.chenharryhua.nanjin.guard.event

import cats.Traverse
import cats.data.Kleisli
import cats.effect.kernel.{Async, Ref}
import cats.effect.std.UUIDGen
import cats.implicits.{catsSyntaxApply, toFunctorOps}
import cats.syntax.all.*
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Importance, ServiceParams}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import fs2.concurrent.Channel
import io.circe.Json
import org.apache.commons.lang3.exception.ExceptionUtils
import retry.RetryDetails
import retry.RetryDetails.WillDelayAndRetry

import java.time.{Duration, ZonedDateTime}
import scala.compat.java8.DurationConverters.FiniteDurationops
import scala.concurrent.duration.FiniteDuration

final private[guard] class EventPublisher[F[_]: UUIDGen](
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  serviceInfo: ServiceInfo,
  serviceParams: ServiceParams)(implicit F: Async[F]) {

  // service level
  private val metricsReportMRName: String = "01.health.check"
  private val serviceStartMRName: String  = "02.service.start"
  private val servicePanicMRName: String  = "03.service.`panic`"

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
        .send(ServiceStarted(timestamp = ts, serviceInfo = serviceInfo, serviceParams = serviceParams))
        .map(_ => metricRegistry.counter(serviceStartMRName).inc()))

  def servicePanic(retryDetails: RetryDetails, ex: Throwable): F[Unit] =
    realZonedDateTime
      .flatMap(ts =>
        channel.send(
          ServicePanic(
            timestamp = ts,
            serviceInfo = serviceInfo,
            serviceParams = serviceParams,
            retryDetails = retryDetails,
            error = NJError(ex))))
      .map(_ => metricRegistry.counter(servicePanicMRName).inc())

  val serviceStopped: F[Unit] =
    realZonedDateTime.flatMap(ts =>
      channel
        .send(
          ServiceStopped(
            timestamp = ts,
            serviceInfo = serviceInfo,
            serviceParams = serviceParams,
            snapshot = MetricsSnapshot(
              metricRegistry = metricRegistry,
              rateTimeUnit = serviceParams.metricsRateTimeUnit,
              durationTimeUnit = serviceParams.metricsDurationTimeUnit,
              zoneId = serviceParams.taskParams.zoneId
            )
          ))
        .void)

  def metricsReport(index: Long, dur: FiniteDuration): F[Unit] =
    F.delay(metricRegistry.counter(metricsReportMRName).inc()) <*
      realZonedDateTime.flatMap(ts =>
        channel.send(
          MetricsReport(
            index = index,
            timestamp = ts,
            serviceInfo = serviceInfo,
            serviceParams = serviceParams,
            prev = Some(ts.minus(dur.toJava)),
            next = Some(ts.plus(dur.toJava)),
            snapshot = MetricsSnapshot(
              metricRegistry = metricRegistry,
              rateTimeUnit = serviceParams.metricsRateTimeUnit,
              durationTimeUnit = serviceParams.metricsDurationTimeUnit,
              zoneId = serviceParams.taskParams.zoneId
            )
          )))

  def metricsReport(index: Long, cronExpr: CronExpr): F[Unit] =
    F.delay(metricRegistry.counter(metricsReportMRName).inc()) <*
      realZonedDateTime.flatMap(ts =>
        channel.send(
          MetricsReport(
            index = index,
            timestamp = ts,
            serviceInfo = serviceInfo,
            serviceParams = serviceParams,
            prev = cronExpr.prev(ts),
            next = cronExpr.next(ts),
            snapshot = MetricsSnapshot(
              metricRegistry = metricRegistry,
              rateTimeUnit = serviceParams.metricsRateTimeUnit,
              durationTimeUnit = serviceParams.metricsDurationTimeUnit,
              zoneId = serviceParams.taskParams.zoneId
            )
          )))

  def metricsReset(cronExpr: CronExpr): F[Unit] =
    realZonedDateTime.flatMap(ts =>
      channel
        .send(
          MetricsReset(
            timestamp = ts,
            serviceInfo = serviceInfo,
            serviceParams = serviceParams,
            prev = cronExpr.prev(ts),
            next = cronExpr.next(ts),
            snapshot = MetricsSnapshot(
              metricRegistry = metricRegistry,
              rateTimeUnit = serviceParams.metricsRateTimeUnit,
              durationTimeUnit = serviceParams.metricsDurationTimeUnit,
              zoneId = serviceParams.taskParams.zoneId
            )
          ))
        .map(_ => metricRegistry.removeMatching(MetricFilter.ALL)))

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
        case Importance.Medium => F.delay(metricRegistry.counter(actionStartMRName(actionParams)).inc())
        case Importance.Low    => F.unit
      }
    } yield actionInfo

  private def timing(name: String, actionInfo: ActionInfo, timestamp: ZonedDateTime): F[Unit] =
    F.delay(metricRegistry.timer(name).update(Duration.between(actionInfo.launchTime, timestamp)))

  def actionSucced[A, B](
    actionInfo: ActionInfo,
    actionParams: ActionParams,
    retryCount: Ref[F, Int],
    input: A,
    output: F[B],
    buildNotes: Kleisli[F, (A, B), String]): F[Unit] =
    actionParams.importance match {
      case Importance.High =>
        for {
          ts <- realZonedDateTime
          result <- output
          num <- retryCount.get
          notes <- buildNotes.run((input, result)).map(Notes(_))
          _ <- channel.send(
            ActionSucced(
              actionInfo = actionInfo,
              timestamp = ts,
              actionParams = actionParams,
              numRetries = num,
              notes = notes))
          _ <- timing(actionSuccMRName(actionParams), actionInfo, ts)
        } yield ()
      case Importance.Medium => realZonedDateTime.flatMap(timing(actionSuccMRName(actionParams), actionInfo, _))
      case Importance.Low    => F.unit
    }

  def quasiSucced[T[_]: Traverse, A, B](
    actionInfo: ActionInfo,
    actionParams: ActionParams,
    runMode: RunMode,
    results: F[(List[(A, NJError)], T[(A, B)])],
    succ: Kleisli[F, List[(A, B)], String],
    fail: Kleisli[F, List[(A, NJError)], String]
  ): F[Unit] =
    actionParams.importance match {
      case Importance.High =>
        for {
          ts <- realZonedDateTime
          res <- results
          sn <- succ.run(res._2.toList)
          fn <- fail.run(res._1)
          _ <- channel.send(
            ActionQuasiSucced(
              actionInfo = actionInfo,
              timestamp = ts,
              actionParams = actionParams,
              runMode = runMode,
              numSucc = res._2.size,
              succNotes = Notes(sn),
              failNotes = Notes(fn),
              errors = res._1.map(_._2)
            ))
          _ <- timing(actionSuccMRName(actionParams), actionInfo, ts)
        } yield ()
      case Importance.Medium => realZonedDateTime.flatMap(timing(actionSuccMRName(actionParams), actionInfo, _))
      case Importance.Low    => F.unit
    }

  def actionRetrying(
    actionInfo: ActionInfo,
    actionParams: ActionParams,
    retryCount: Ref[F, Int],
    willDelayAndRetry: WillDelayAndRetry,
    ex: Throwable
  ): F[Unit] =
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
          })) *> retryCount.update(_ + 1)

  def actionFailed[A](
    actionInfo: ActionInfo,
    actionParams: ActionParams,
    retryCount: Ref[F, Int],
    input: A,
    ex: Throwable,
    buildNotes: Kleisli[F, (A, Throwable), String]
  ): F[Unit] =
    for {
      ts <- realZonedDateTime
      numRetries <- retryCount.get
      notes <- buildNotes.run((input, ex)).map(Notes(_))
      _ <- channel.send(
        ActionFailed(
          actionInfo = actionInfo,
          timestamp = ts,
          actionParams = actionParams,
          numRetries = numRetries,
          notes = notes,
          error = NJError(ex)))
      _ <- actionParams.importance match {
        case Importance.High | Importance.Medium => timing(actionFailMRName(actionParams), actionInfo, ts)
        case Importance.Low                      => F.unit
      }
    } yield ()

  def quasiFailed(actionInfo: ActionInfo, actionParams: ActionParams, ex: Throwable): F[Unit] =
    for {
      ts <- realZonedDateTime
      _ <- channel.send(
        ActionFailed(
          actionInfo = actionInfo,
          timestamp = ts,
          actionParams = actionParams,
          numRetries = 0,
          notes = Notes(ExceptionUtils.getMessage(ex)),
          error = NJError(ex)))
      _ <- actionParams.importance match {
        case Importance.High | Importance.Medium => timing(actionFailMRName(actionParams), actionInfo, ts)
        case Importance.Low                      => F.unit
      }
    } yield ()

  def passThrough(actionParams: ActionParams, json: Json): F[Unit] =
    realZonedDateTime.flatMap(ts =>
      channel
        .send(PassThrough(ts, actionParams, json))
        .map(_ => metricRegistry.counter(passThroughMRName(actionParams)).inc()))

  def count(actionParams: ActionParams, num: Long): F[Unit] =
    F.delay(metricRegistry.counter(counterMRName(actionParams)).inc(num))
}
