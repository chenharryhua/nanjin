package com.github.chenharryhua.nanjin.guard.event

import cats.data.Kleisli
import cats.effect.kernel.{Async, Ref}
import cats.effect.std.UUIDGen
import cats.implicits.{catsSyntaxApply, toFunctorOps}
import cats.syntax.all.*
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Importance, MetricName}
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import fs2.concurrent.Channel
import io.circe.Json
import retry.RetryDetails
import retry.RetryDetails.WillDelayAndRetry

import java.time.{Duration, ZonedDateTime}

private[guard] object EventPublisher {
  final val ATTENTION = "02.attention"
}

final private[guard] class EventPublisher[F[_]](
  val serviceInfo: ServiceInfo,
  val metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent])(implicit F: Async[F]) {
  import EventPublisher.ATTENTION

  // service level
  private val metricsReportMRName: String = "01.health.check"
  private val servicePanicMRName: String  = s"$ATTENTION.service.panic"
  private val serviceStartMRName: String  = "03.service.start"

  private def alertMRName(name: MetricName, importance: Importance): String =
    importance match {
      case Importance.Critical => s"$ATTENTION.alert.error.[${name.value}]"
      case Importance.High     => s"10.alert.warn.[${name.value}]"
      case Importance.Medium   => s"10.alert.info.[${name.value}]"
      case Importance.Low      => s"10.alert.debug.[${name.value}]"
    }

  // action level
  private def counterMRName(name: MetricName, isError: Boolean): String =
    if (isError) s"$ATTENTION.counter.[${name.value}]" else s"20.counter.[${name.value}]"

  private def passThroughMRName(name: MetricName, isError: Boolean): String =
    if (isError) s"$ATTENTION.pass.through.[${name.value}]" else s"21.pass.through.[${name.value}]"

  private def actionFailMRName(params: ActionParams): String = s"$ATTENTION.action.[${params.metricName.value}].failure"
  private def actionRetryMRName(params: ActionParams): String = s"30.action.[${params.metricName.value}].retries"
  private def actionStartMRName(params: ActionParams): String = s"30.action.[${params.metricName.value}].started"
  private def actionSuccMRName(params: ActionParams): String  = s"30.action.[${params.metricName.value}].success"

  private val realZonedDateTime: F[ZonedDateTime] =
    F.realTimeInstant.map(_.atZone(serviceInfo.params.taskParams.zoneId))

  /** services
    */

  val serviceReStarted: F[Unit] =
    realZonedDateTime.flatMap(ts =>
      channel
        .send(ServiceStarted(timestamp = ts, serviceInfo = serviceInfo))
        .map(_ => metricRegistry.counter(serviceStartMRName).inc()))

  def servicePanic(retryDetails: RetryDetails, ex: Throwable): F[Unit] =
    for {
      ts <- realZonedDateTime
      uuid <- UUIDGen.randomUUID[F]
      _ <- channel.send(
        ServicePanic(timestamp = ts, serviceInfo = serviceInfo, retryDetails = retryDetails, error = NJError(uuid, ex)))
    } yield metricRegistry.counter(servicePanicMRName).inc()

  def serviceStopped(metricFilter: MetricFilter): F[Unit] =
    for {
      ts <- realZonedDateTime
      _ <- channel.send(
        ServiceStopped(
          timestamp = ts,
          serviceInfo = serviceInfo,
          snapshot = MetricsSnapshot(metricRegistry, metricFilter, serviceInfo.params)
        ))
    } yield ()

  def metricsReport(metricFilter: MetricFilter, metricReportType: MetricReportType): F[Unit] =
    for {
      _ <- F.delay(metricRegistry.counter(metricsReportMRName).inc())
      ts <- realZonedDateTime
      _ <- channel.send(
        MetricsReport(
          serviceInfo = serviceInfo,
          reportType = metricReportType,
          timestamp = ts,
          snapshot = MetricsSnapshot(metricRegistry, metricFilter, serviceInfo.params)
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
            snapshot = MetricsSnapshot(metricRegistry, metricFilter, serviceInfo.params)
          )
        }
      }.getOrElse(MetricsReset(
        resetType = MetricResetType.AdventiveReset,
        serviceInfo = serviceInfo,
        timestamp = ts,
        snapshot = MetricsSnapshot(metricRegistry, metricFilter, serviceInfo.params)
      ))
      _ <- channel.send(msg)
    } yield metricRegistry.removeMatching(MetricFilter.ALL)

  /** actions
    */

  def actionStart(actionParams: ActionParams): F[ActionInfo] =
    for {
      uuid <- UUIDGen.randomUUID[F]
      ts <- realZonedDateTime
      actionInfo = ActionInfo(actionParams, serviceInfo, uuid, ts)
      _ <- actionParams.importance match {
        case Importance.Critical | Importance.High =>
          channel.send(ActionStart(actionInfo)).map(_ => metricRegistry.counter(actionStartMRName(actionParams)).inc())
        case Importance.Medium => F.delay(metricRegistry.counter(actionStartMRName(actionParams)).inc())
        case Importance.Low    => F.unit
      }
    } yield actionInfo

  private def timing(name: String, actionInfo: ActionInfo, timestamp: ZonedDateTime): F[Unit] =
    F.delay(metricRegistry.timer(name).update(Duration.between(actionInfo.launchTime, timestamp)))

  def actionSucced[A, B](
    actionInfo: ActionInfo,
    retryCount: Ref[F, Int],
    input: A,
    output: F[B],
    buildNotes: Kleisli[F, (A, B), String]): F[Unit] =
    actionInfo.params.importance match {
      case Importance.Critical | Importance.High =>
        for {
          ts <- realZonedDateTime
          result <- output
          num <- retryCount.get
          notes <- buildNotes.run((input, result))
          _ <- channel.send(
            ActionSucced(actionInfo = actionInfo, timestamp = ts, numRetries = num, notes = Notes(notes)))
          _ <- timing(actionSuccMRName(actionInfo.params), actionInfo, ts)
        } yield ()
      case Importance.Medium =>
        realZonedDateTime.flatMap(ts => timing(actionSuccMRName(actionInfo.params), actionInfo, ts))
      case Importance.Low => F.unit
    }

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
      _ <- actionInfo.params.importance match {
        case Importance.Critical | Importance.High | Importance.Medium =>
          timing(actionRetryMRName(actionInfo.params), actionInfo, ts)
        case Importance.Low => F.unit
      }
      _ <- retryCount.update(_ + 1)
    } yield ()

  def actionFailed[A](
    actionInfo: ActionInfo,
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
          numRetries = numRetries,
          notes = Notes(notes),
          error = NJError(uuid, ex)))
      _ <- actionInfo.params.importance match {
        case Importance.Critical | Importance.High | Importance.Medium =>
          timing(actionFailMRName(actionInfo.params), actionInfo, ts)
        case Importance.Low => F.unit
      }
    } yield ()

  def passThrough(metricName: MetricName, json: Json, isError: Boolean): F[Unit] =
    for {
      ts <- realZonedDateTime
      _ <- channel.send(
        PassThrough(
          metricName = metricName,
          isError = isError,
          serviceInfo = serviceInfo,
          timestamp = ts,
          value = json))
    } yield metricRegistry.counter(passThroughMRName(metricName, isError)).inc()

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
    } yield metricRegistry.counter(alertMRName(metricName, importance)).inc()

  def increase(metricName: MetricName, num: Long, isError: Boolean): F[Unit] =
    F.delay(metricRegistry.counter(counterMRName(metricName, isError)).inc(num))

  def replace(metricName: MetricName, num: Long, isError: Boolean): F[Unit] = F.delay {
    val name = counterMRName(metricName, isError)
    val old  = metricRegistry.counter(name).getCount
    metricRegistry.counter(name).inc(num)
    metricRegistry.counter(name).dec(old)
  }
}
