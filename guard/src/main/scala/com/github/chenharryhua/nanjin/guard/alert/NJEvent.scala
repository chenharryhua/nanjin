package com.github.chenharryhua.nanjin.guard.alert

import cats.Show
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, ServiceParams}
import io.circe.generic.auto._
import io.circe.shapes._
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils
import retry.RetryDetails
import retry.RetryDetails.WillDelayAndRetry

import java.time.ZonedDateTime
import java.util.UUID

final case class ServiceInfo(hostName: String, id: UUID, launchTime: ZonedDateTime)

final case class ActionInfo(actionName: String, serviceInfo: ServiceInfo, id: UUID, launchTime: ZonedDateTime)

final case class Notes private (value: String)

object Notes {
  def apply(str: String): Notes = new Notes(Option(str).getOrElse("null in notes"))
}

final case class NJError private (id: UUID, message: String, stackTrace: String, throwable: Throwable)

object NJError {
  implicit val showNJError: Show[NJError] = _.message

  implicit val encodeNJError: Encoder[NJError] = (a: NJError) =>
    Json.obj(
      ("id", Json.fromString(a.id.toString)),
      ("message", Json.fromString(a.message)),
      ("stackTrace", Json.fromString(a.stackTrace))
    )

  implicit val decodeNJError: Decoder[NJError] = (c: HCursor) =>
    for {
      id <- c.downField("id").as[UUID]
      msg <- c.downField("message").as[String]
      st <- c.downField("stackTrace").as[String]
    } yield NJError(id, msg, st, new Throwable("fake Throwable")) // can not recover throwables.

  def apply(ex: Throwable): NJError =
    NJError(UUID.randomUUID(), ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex), ex)
}

final case class DailySummaries private (actionSucc: Long, actionFail: Long, actionRetries: Long, servicePanic: Long) {
  def incServicePanic: DailySummaries  = copy(servicePanic = servicePanic + 1)
  def incActionSucc: DailySummaries    = copy(actionSucc = actionSucc + 1)
  def incActionFail: DailySummaries    = copy(actionFail = actionFail + 1)
  def incActionRetries: DailySummaries = copy(actionRetries = actionRetries + 1)
  def reset: DailySummaries            = DailySummaries.zero
}

object DailySummaries {
  val zero: DailySummaries = DailySummaries(0L, 0L, 0L, 0L)
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
  dailySummaries: DailySummaries,
  totalMemory: Long,
  freeMemory: Long
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
  numRetries: Int, // number of retries before giving up
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

final case class ActionQuasiSucced(
  timestamp: ZonedDateTime,
  actionInfo: ActionInfo,
  params: ActionParams,
  numSucc: Long,
  succNotes: Notes,
  failNotes: Notes,
  errors: List[NJError]
) extends ActionEvent

final case class ForYourInformation(timestamp: ZonedDateTime, message: String) extends NJEvent
final case class PassThrough(timestamp: ZonedDateTime, value: Json) extends NJEvent
