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

final private[guard] class EventPublisher[F[_]](
  val serviceInfo: ServiceInfo,
  val metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent])(implicit F: Async[F]) {

  // service level
  private val metricsReportMRName: String = "01.health.check"
  private val serviceStartMRName: String  = "02.service.start"
  private val servicePanicMRName: String  = "03.service.`panic`"
  private def alertMRName(name: MetricName, importance: Importance): String =
    importance match {
      case Importance.Critical => s"04.alert.`error`.[${name.value}]"
      case Importance.High     => s"04.alert.`warn`.[${name.value}]"
      case Importance.Medium   => s"20.alert.info.[${name.value}]"
      case Importance.Low      => s"20.alert.debug.[${name.value}]"
    }

  // action level
  private def counterMRName(name: MetricName): String     = s"10.counter.[${name.value}]"
  private def passThroughMRName(name: MetricName): String = s"11.pass.through.[${name.value}]"

  private def actionFailMRName(params: ActionParams): String  = s"12.action.[${params.metricName.value}].`fail`"
  private def actionRetryMRName(params: ActionParams): String = s"12.action.[${params.metricName.value}].retry"
  private def actionStartMRName(params: ActionParams): String = s"12.action.[${params.metricName.value}].num"
  private def actionSuccMRName(params: ActionParams): String  = s"12.action.[${params.metricName.value}].succ"

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

  def metricsReport(metricFilter: MetricFilter, index: Long): F[Unit] =
    for {
      _ <- F.delay(metricRegistry.counter(metricsReportMRName).inc())
      ts <- realZonedDateTime
      _ <- channel.send(
        MetricsReport(
          index = index,
          timestamp = ts,
          serviceInfo = serviceInfo,
          snapshot = MetricsSnapshot(metricRegistry, metricFilter, serviceInfo.params)
        ))
    } yield ()

  def metricsReset(metricFilter: MetricFilter, cronExpr: Option[CronExpr]): F[Unit] = cronExpr match {
    case Some(cron) =>
      for {
        ts <- realZonedDateTime
        _ <- channel.send(
          MetricsReset(
            timestamp = ts,
            serviceInfo = serviceInfo,
            prev = cron.prev(ts),
            next = cron.next(ts),
            snapshot = MetricsSnapshot(metricRegistry, metricFilter, serviceInfo.params)
          ))
      } yield metricRegistry.removeMatching(MetricFilter.ALL)
    case None =>
      for {
        ts <- realZonedDateTime
        _ <- channel.send(
          MetricsReset(
            timestamp = ts,
            serviceInfo = serviceInfo,
            prev = None,
            next = None,
            snapshot = MetricsSnapshot(metricRegistry, metricFilter, serviceInfo.params)
          ))
      } yield metricRegistry.removeMatching(MetricFilter.ALL)
  }

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

  def passThrough(metricName: MetricName, json: Json): F[Unit] =
    for {
      ts <- realZonedDateTime
      _ <- channel.send(PassThrough(metricName = metricName, serviceInfo = serviceInfo, timestamp = ts, value = json))
    } yield metricRegistry.counter(passThroughMRName(metricName)).inc()

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

  def increase(metricName: MetricName, num: Long): F[Unit] =
    F.delay(metricRegistry.counter(counterMRName(metricName)).inc(num))

  def replace(metricName: MetricName, num: Long): F[Unit] = F.delay {
    val exist = metricRegistry.counter(counterMRName(metricName)).getCount
    metricRegistry.counter(counterMRName(metricName)).inc(num)
    metricRegistry.counter(counterMRName(metricName)).dec(exist)
  }
}
