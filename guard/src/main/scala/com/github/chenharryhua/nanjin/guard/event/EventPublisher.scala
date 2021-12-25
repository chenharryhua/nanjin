package com.github.chenharryhua.nanjin.guard.event

import cats.data.Kleisli
import cats.effect.kernel.{Ref, Temporal}
import cats.effect.std.UUIDGen
import cats.implicits.{catsSyntaxApply, toFunctorOps}
import cats.syntax.all.*
import com.codahale.metrics.{Counter, MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Importance, MetricName}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import fs2.concurrent.Channel
import io.circe.Json
import retry.RetryDetails
import retry.RetryDetails.WillDelayAndRetry

import java.time.ZonedDateTime
import scala.jdk.CollectionConverters.SetHasAsScala

final private[guard] class EventPublisher[F[_]: UUIDGen](
  val serviceInfo: ServiceInfo,
  val metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent])(implicit F: Temporal[F]) {

  private val realZonedDateTime: F[ZonedDateTime] =
    F.realTimeInstant.map(_.atZone(serviceInfo.serviceParams.taskParams.zoneId))

  /** services
    */

  val serviceReStarted: F[Unit] =
    realZonedDateTime.flatMap(ts => channel.send(ServiceStarted(timestamp = ts, serviceInfo = serviceInfo)).void)

  def servicePanic(retryDetails: RetryDetails, ex: Throwable): F[Unit] =
    for {
      ts <- realZonedDateTime
      uuid <- UUIDGen.randomUUID[F]
      _ <- channel.send(ServicePanic(serviceInfo, ts, retryDetails, NJError(uuid, ex)))
    } yield ()

  def serviceStopped(metricFilter: MetricFilter): F[Unit] =
    for {
      ts <- realZonedDateTime
      _ <- channel.send(
        ServiceStopped(
          timestamp = ts,
          serviceInfo = serviceInfo,
          snapshot = MetricsSnapshot(metricRegistry, metricFilter, serviceInfo.serviceParams)
        ))
    } yield ()

  def metricsReport(metricFilter: MetricFilter, metricReportType: MetricReportType): F[Unit] =
    for {
      ts <- realZonedDateTime
      _ <- channel.send(
        MetricsReport(
          serviceInfo = serviceInfo,
          reportType = metricReportType,
          timestamp = ts,
          snapshot = MetricsSnapshot(metricRegistry, metricFilter, serviceInfo.serviceParams)
        ))
    } yield ()

  def metricsReset(metricFilter: MetricFilter, cronExpr: Option[CronExpr]): F[Unit] =
    for {
      ts <- realZonedDateTime
      msg = cronExpr.flatMap { ce =>
        (ce.prev(ts), ce.next(ts)).mapN { case (prev, next) =>
          MetricsReset(
            resetType = MetricResetType.ScheduledReset(prev, next),
            serviceInfo = serviceInfo,
            timestamp = ts,
            snapshot = MetricsSnapshot(metricRegistry, metricFilter, serviceInfo.serviceParams)
          )
        }
      }.getOrElse(MetricsReset(
        resetType = MetricResetType.AdventiveReset,
        serviceInfo = serviceInfo,
        timestamp = ts,
        snapshot = MetricsSnapshot(metricRegistry, metricFilter, serviceInfo.serviceParams)
      ))
      _ <- channel.send(msg)
    } yield metricRegistry.getCounters(metricFilter).keySet().asScala.foreach { c =>
      val entry: Counter = metricRegistry.counter(c)
      entry.dec(entry.getCount)
    }

  /** actions
    */

  def actionStart(actionParams: ActionParams): F[ActionInfo] =
    for {
      uuid <- UUIDGen.randomUUID[F]
      ts <- realZonedDateTime
      actionInfo = ActionInfo(actionParams, serviceInfo, uuid, ts)
      _ <- channel.send(ActionStart(actionInfo)).whenA(actionInfo.actionParams.importance >= Importance.High)
    } yield actionInfo

  def actionSucced[A, B](
    actionInfo: ActionInfo,
    retryCount: Ref[F, Int],
    input: A,
    output: F[B],
    buildNotes: Kleisli[F, (A, B), String]): F[ZonedDateTime] =
    for {
      ts <- realZonedDateTime
      result <- output
      num <- retryCount.get
      notes <- buildNotes.run((input, result))
      _ <- channel
        .send(ActionSucced(actionInfo = actionInfo, timestamp = ts, numRetries = num, notes = Notes(notes)))
        .whenA(actionInfo.actionParams.importance >= Importance.High)
    } yield ts

  def actionRetrying(
    actionInfo: ActionInfo,
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
          willDelayAndRetry = willDelayAndRetry,
          error = NJError(uuid, ex)))
      _ <- retryCount.update(_ + 1)
    } yield ()

  def actionFailed[A](
    actionInfo: ActionInfo,
    retryCount: Ref[F, Int],
    input: A,
    ex: Throwable,
    buildNotes: Kleisli[F, (A, Throwable), String]
  ): F[ZonedDateTime] =
    for {
      ts <- realZonedDateTime
      uuid <- UUIDGen.randomUUID[F]
      numRetries <- retryCount.get
      notes <- buildNotes.run((input, ex))
      _ <- channel.send(
        ActionFailed(
          actionInfo = actionInfo,
          timestamp = ts,
          numRetries = numRetries,
          notes = Notes(notes),
          error = NJError(uuid, ex)))
    } yield ts

  def passThrough(metricName: MetricName, json: Json, asError: Boolean): F[Unit] =
    for {
      ts <- realZonedDateTime
      _ <- channel.send(
        PassThrough(
          metricName = metricName,
          asError = asError,
          serviceInfo = serviceInfo,
          timestamp = ts,
          value = json))
    } yield ()

  def alert(metricName: MetricName, msg: String, importance: Importance): F[Unit] =
    for {
      ts <- realZonedDateTime
      _ <- channel.send(
        ServiceAlert(
          metricName = metricName,
          serviceInfo = serviceInfo,
          timestamp = ts,
          importance = importance,
          message = msg))
    } yield ()
}
