package com.github.chenharryhua.nanjin.guard.alert

import cats.Show
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, ServiceParams}
import io.circe.generic.auto._
import io.circe.shapes._
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils
import retry.RetryDetails
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.ZonedDateTime
import java.util.UUID

final case class ServiceInfo(serviceName: String, appName: String, params: ServiceParams, launchTime: ZonedDateTime) {
  def metricsKey: String = s"$serviceName.$appName"
}

final case class ActionInfo(
  actionName: String,
  serviceName: String,
  appName: String,
  params: ActionParams,
  id: UUID,
  launchTime: ZonedDateTime) {
  def metricsKey: String = s"$actionName.$serviceName.$appName"
}

final case class Notes private (value: String)

object Notes {
  def apply(str: String): Notes = new Notes(Option(str).getOrElse("null in notes"))
}

final case class NJError private (message: String, stackTrace: String, throwable: Throwable)

object NJError {
  implicit val showNJError: Show[NJError] = _.message

  implicit val encodeNJError: Encoder[NJError] = (a: NJError) =>
    Json.obj(
      ("message", Json.fromString(a.message)),
      ("stackTrace", Json.fromString(a.stackTrace))
    )

  implicit val decodeNJError: Decoder[NJError] = (c: HCursor) =>
    for {
      msg <- c.downField("message").as[String]
      st <- c.downField("stackTrace").as[String]
    } yield NJError(msg, st, new Throwable("fake Throwable")) // can not recover throwables.

  def apply(ex: Throwable): NJError =
    NJError(ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex), ex)
}

final case class DailySummaries(actionSucc: Int, actionFail: Int, actionRetries: Int, servicePanic: Int) {
  def incServicePanic: DailySummaries  = copy(servicePanic = servicePanic + 1)
  def incActionSucc: DailySummaries    = copy(actionSucc = actionSucc + 1)
  def incActionFail: DailySummaries    = copy(actionFail = actionFail + 1)
  def incActionRetries: DailySummaries = copy(actionRetries = actionRetries + 1)
  def reset: DailySummaries            = DailySummaries.zero
}

object DailySummaries {
  val zero: DailySummaries = DailySummaries(0, 0, 0, 0)
}

sealed trait NJEvent

object NJEvent {
  implicit val showNJEvent: Show[NJEvent]       = cats.derived.semiauto.show[NJEvent]
  implicit val encoderNJEvent: Encoder[NJEvent] = io.circe.generic.semiauto.deriveEncoder[NJEvent]
  implicit val decoderNJEvent: Decoder[NJEvent] = io.circe.generic.semiauto.deriveDecoder[NJEvent]
}

sealed trait ServiceEvent extends NJEvent {
  def serviceInfo: ServiceInfo
}

final case class ServiceStarted(serviceInfo: ServiceInfo) extends ServiceEvent

final case class ServicePanic(
  serviceInfo: ServiceInfo,
  retryDetails: RetryDetails,
  errorID: UUID,
  error: NJError
) extends ServiceEvent

final case class ServiceStopped(
  serviceInfo: ServiceInfo
) extends ServiceEvent

final case class ServiceHealthCheck(
  serviceInfo: ServiceInfo,
  dailySummaries: DailySummaries
) extends ServiceEvent

sealed trait ActionEvent extends NJEvent {
  def actionInfo: ActionInfo
}

final case class ActionRetrying(
  actionInfo: ActionInfo,
  willDelayAndRetry: WillDelayAndRetry,
  error: NJError
) extends ActionEvent

final case class ActionFailed(
  actionInfo: ActionInfo,
  givingUp: GivingUp,
  endAt: ZonedDateTime, // computation finished
  notes: Notes, // failure notes
  error: NJError
) extends ActionEvent

final case class ActionSucced(
  actionInfo: ActionInfo,
  endAt: ZonedDateTime, // computation finished
  numRetries: Int, // how many retries before success
  notes: Notes // success notes
) extends ActionEvent

final case class ForYouInformation(message: String) extends NJEvent
