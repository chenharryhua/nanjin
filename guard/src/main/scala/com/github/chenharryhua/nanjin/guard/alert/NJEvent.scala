package com.github.chenharryhua.nanjin.guard.alert

import cats.Show
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, ServiceParams}
import io.circe.generic.auto._
import io.circe.shapes._
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils
import retry.RetryDetails
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.Instant
import java.util.UUID

final case class ServiceInfo(serviceName: String, appName: String, params: ServiceParams, launchTime: Instant) {
  def metricsKey: String = s"service.$serviceName.$appName"
}

final case class ActionInfo(
  actionName: String,
  serviceName: String,
  appName: String,
  params: ActionParams,
  id: UUID,
  launchTime: Instant) {
  def metricsKey: String = s"action.$actionName.$serviceName.$appName"
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
      m <- c.downField("message").as[String]
      sf <- c.downField("stackTrace").as[String]
    } yield NJError(m, sf, new Throwable("fake Throwable"))

  def apply(ex: Throwable): NJError =
    NJError(ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex), ex)
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

final case class ServiceStoppedAbnormally(
  serviceInfo: ServiceInfo
) extends ServiceEvent

final case class ServiceHealthCheck(
  serviceInfo: ServiceInfo
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
  endAt: Instant, // computation finished
  notes: Notes, // failure notes
  error: NJError
) extends ActionEvent

final case class ActionSucced(
  actionInfo: ActionInfo,
  endAt: Instant, // computation finished
  numRetries: Int, // how many retries before success
  notes: Notes // success notes
) extends ActionEvent

final case class ForYouInformation(message: String) extends NJEvent
