package com.github.chenharryhua.nanjin.guard.event

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
import retry.RetryDetails
import retry.RetryDetails.WillDelayAndRetry

import java.time.{Duration, ZonedDateTime}

final private[guard] class EventPublisher[F[_]](
  val serviceInfo: ServiceInfo,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams)(implicit F: Async[F]) {

  // service level
  private val metricsReportMRName: String       = "01.health.check"
  private val serviceStartMRName: String        = "02.service.start"
  private val servicePanicMRName: String        = "03.service.`panic`"
  private def alertMRName(name: String): String = s"04.`alert`.[$name]"

  // action level
  private def counterMRName(name: String): String     = s"10.counter.[$name]"
  private def passThroughMRName(name: String): String = s"11.pass.through.[$name]"

  private def actionFailMRName(name: String): String  = s"12.action.[$name].`fail`"
  private def actionRetryMRName(name: String): String = s"12.action.[$name].retry"
  private def actionStartMRName(name: String): String = s"12.action.[$name].num"
  private def actionSuccMRName(name: String): String  = s"12.action.[$name].succ"

  private val realZonedDateTime: F[ZonedDateTime] = F.realTimeInstant.map(_.atZone(serviceParams.taskParams.zoneId))

  /** services
    */

  val serviceReStarted: F[Unit] =
    realZonedDateTime.flatMap(ts =>
      channel
        .send(ServiceStarted(timestamp = ts, serviceInfo = serviceInfo, serviceParams = serviceParams))
        .map(_ => metricRegistry.counter(serviceStartMRName).inc()))

  def servicePanic(retryDetails: RetryDetails, ex: Throwable): F[Unit] =
    for {
      ts <- realZonedDateTime
      uuid <- UUIDGen.randomUUID[F]
      _ <- channel.send(
        ServicePanic(
          timestamp = ts,
          serviceInfo = serviceInfo,
          serviceParams = serviceParams,
          retryDetails = retryDetails,
          error = NJError(uuid, ex)))
    } yield metricRegistry.counter(servicePanicMRName).inc()

  def serviceStopped(metricFilter: MetricFilter): F[Unit] =
    for {
      ts <- realZonedDateTime
      _ <- channel.send(
        ServiceStopped(
          timestamp = ts,
          serviceInfo = serviceInfo,
          serviceParams = serviceParams,
          snapshot = MetricsSnapshot(metricRegistry, metricFilter, serviceParams)
        ))
    } yield ()

  def metricsReport(metricFilter: MetricFilter, index: Long): F[Unit] =
    for {
      _ <- F.delay(metricRegistry.counter(metricsReportMRName).inc())
      ts <- realZonedDateTime
      _ <- channel.send(
        MetricsReport(
          index = index,
          timestamp = ts,
          serviceInfo = serviceInfo,
          serviceParams = serviceParams,
          snapshot = MetricsSnapshot(metricRegistry, metricFilter, serviceParams)
        ))
    } yield ()

  def metricsReset(metricFilter: MetricFilter, cronExpr: CronExpr): F[Unit] =
    for {
      ts <- realZonedDateTime
      _ <- channel.send(
        MetricsReset(
          timestamp = ts,
          serviceInfo = serviceInfo,
          serviceParams = serviceParams,
          prev = cronExpr.prev(ts),
          next = cronExpr.next(ts),
          snapshot = MetricsSnapshot(metricRegistry, metricFilter, serviceParams)
        ))
    } yield metricRegistry.removeMatching(MetricFilter.ALL)

  /** actions
    */

  def actionStart(actionParams: ActionParams): F[ActionInfo] =
    for {
      uuid <- UUIDGen.randomUUID[F]
      ts <- realZonedDateTime
      actionInfo = ActionInfo(uuid, ts)
      _ <- actionParams.importance match {
        case Importance.High =>
          channel
            .send(ActionStart(actionParams, actionInfo))
            .map(_ => metricRegistry.counter(actionStartMRName(actionParams.actionName)).inc())
        case Importance.Medium => F.delay(metricRegistry.counter(actionStartMRName(actionParams.actionName)).inc())
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
          notes <- buildNotes.run((input, result))
          _ <- channel.send(
            ActionSucced(
              actionInfo = actionInfo,
              timestamp = ts,
              actionParams = actionParams,
              numRetries = num,
              notes = Notes(notes)))
          _ <- timing(actionSuccMRName(actionParams.actionName), actionInfo, ts)
        } yield ()
      case Importance.Medium =>
        realZonedDateTime.flatMap(ts => timing(actionSuccMRName(actionParams.actionName), actionInfo, ts))
      case Importance.Low => F.unit
    }

  def actionRetrying(
    actionInfo: ActionInfo,
    actionParams: ActionParams,
    retryCount: Ref[F, Int],
    willDelayAndRetry: WillDelayAndRetry,
    ex: Throwable
  ): F[Unit] =
    for {
      ts <- realZonedDateTime
      uuid <- UUIDGen.randomUUID[F]
      _ <- channel.send(
        ActionRetrying(
          actionInfo = actionInfo,
          timestamp = ts,
          actionParams = actionParams,
          willDelayAndRetry = willDelayAndRetry,
          error = NJError(uuid, ex)))
      _ <- actionParams.importance match {
        case Importance.High | Importance.Medium =>
          timing(actionRetryMRName(actionParams.actionName), actionInfo, ts)
        case Importance.Low => F.unit
      }
      _ <- retryCount.update(_ + 1)
    } yield ()

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
      uuid <- UUIDGen.randomUUID[F]
      numRetries <- retryCount.get
      notes <- buildNotes.run((input, ex))
      _ <- channel.send(
        ActionFailed(
          actionInfo = actionInfo,
          timestamp = ts,
          actionParams = actionParams,
          numRetries = numRetries,
          notes = Notes(notes),
          error = NJError(uuid, ex)))
      _ <- actionParams.importance match {
        case Importance.High | Importance.Medium => timing(actionFailMRName(actionParams.actionName), actionInfo, ts)
        case Importance.Low                      => F.unit
      }
    } yield ()

  def passThrough(metricName: String, json: Json): F[Unit] =
    for {
      ts <- realZonedDateTime
      _ <- channel.send(PassThrough(timestamp = ts, metricName = metricName, value = json))
    } yield metricRegistry.counter(passThroughMRName(metricName)).inc()

  def count(metricName: String, num: Long): F[Unit] =
    F.delay(metricRegistry.counter(counterMRName(metricName)).inc(num))

  def alert(alertName: String, msg: String): F[Unit] =
    for {
      ts <- realZonedDateTime
      _ <- channel.send(
        ServiceAlert(
          timestamp = ts,
          serviceInfo = serviceInfo,
          serviceParams = serviceParams,
          alertName = alertName,
          message = msg))
    } yield metricRegistry.counter(alertMRName(alertName)).inc()
}
