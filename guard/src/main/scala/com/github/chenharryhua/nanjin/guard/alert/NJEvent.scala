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

final case class ServiceInfo(hostName: String, id: UUID, launchTime: ZonedDateTime)

final case class ActionInfo(actionName: String, serviceInfo: ServiceInfo, id: UUID, launchTime: ZonedDateTime)

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

sealed trait NJEvent {
  def timestamp: ZonedDateTime // event timestamp - when the event occurs
}

object NJEvent {
  implicit val showNJEvent: Show[NJEvent]       = cats.derived.semiauto.show[NJEvent]
  implicit val encoderNJEvent: Encoder[NJEvent] = io.circe.generic.semiauto.deriveEncoder[NJEvent]
  implicit val decoderNJEvent: Decoder[NJEvent] = io.circe.generic.semiauto.deriveDecoder[NJEvent]
}

sealed trait ServiceEvent extends NJEvent {
  def serviceInfo: ServiceInfo
  def params: ServiceParams
}

final case class ServiceStarted(timestamp: ZonedDateTime, serviceInfo: ServiceInfo, params: ServiceParams)
    extends ServiceEvent

final case class ServicePanic(
  timestamp: ZonedDateTime,
  serviceInfo: ServiceInfo,
  params: ServiceParams,
  retryDetails: RetryDetails,
  errorID: UUID,
  error: NJError
) extends ServiceEvent

final case class ServiceStopped(
  timestamp: ZonedDateTime,
  serviceInfo: ServiceInfo,
  params: ServiceParams
) extends ServiceEvent

final case class ServiceHealthCheck(
  timestamp: ZonedDateTime,
  serviceInfo: ServiceInfo,
  params: ServiceParams,
  dailySummaries: DailySummaries
) extends ServiceEvent

sealed trait ActionEvent extends NJEvent {
  def actionInfo: ActionInfo
  def params: ActionParams
}

final case class ActionRetrying(
  timestamp: ZonedDateTime,
  actionInfo: ActionInfo,
  params: ActionParams,
  willDelayAndRetry: WillDelayAndRetry,
  error: NJError
) extends ActionEvent

final case class ActionFailed(
  timestamp: ZonedDateTime,
  actionInfo: ActionInfo,
  params: ActionParams,
  givingUp: GivingUp,
  notes: Notes, // failure notes
  error: NJError
) extends ActionEvent

final case class ActionSucced(
  timestamp: ZonedDateTime,
  actionInfo: ActionInfo,
  params: ActionParams,
  numRetries: Int, // how many retries before success
  notes: Notes // success notes
) extends ActionEvent

final case class ForYouInformation(timestamp: ZonedDateTime, message: String) extends NJEvent
