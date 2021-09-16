package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, ServiceParams}
import io.circe.generic.auto.*
import io.circe.shapes.*
import io.circe.{Decoder, Encoder, Json}
import retry.RetryDetails
import retry.RetryDetails.WillDelayAndRetry

import java.time.ZonedDateTime

sealed trait NJEvent {
  def timestamp: ZonedDateTime // event timestamp - when the event occurs
  final def show: String = NJEvent.showNJEvent.show(this)
  final def asJson: Json = NJEvent.encoderNJEvent.apply(this)
}

object NJEvent {
  import com.github.chenharryhua.nanjin.datetime.*

  implicit val showNJEvent: Show[NJEvent]       = cats.derived.semiauto.show[NJEvent]
  implicit val encoderNJEvent: Encoder[NJEvent] = io.circe.generic.semiauto.deriveEncoder[NJEvent]
  implicit val decoderNJEvent: Decoder[NJEvent] = io.circe.generic.semiauto.deriveDecoder[NJEvent]
}

sealed trait ServiceEvent extends NJEvent {
  def serviceInfo: ServiceInfo // service runtime infomation
  def serviceParams: ServiceParams // service static parameters
}

final case class ServiceStarted(timestamp: ZonedDateTime, serviceInfo: ServiceInfo, serviceParams: ServiceParams)
    extends ServiceEvent

final case class ServicePanic(
  timestamp: ZonedDateTime,
  serviceInfo: ServiceInfo,
  serviceParams: ServiceParams,
  retryDetails: RetryDetails,
  error: NJError
) extends ServiceEvent

final case class ServiceStopped(
  timestamp: ZonedDateTime,
  serviceInfo: ServiceInfo,
  serviceParams: ServiceParams,
  snapshot: MetricsSnapshot
) extends ServiceEvent

final case class MetricsReport(
  index: Long,
  timestamp: ZonedDateTime,
  serviceInfo: ServiceInfo,
  serviceParams: ServiceParams,
  prev: Option[ZonedDateTime],
  next: Option[ZonedDateTime],
  snapshot: MetricsSnapshot
) extends ServiceEvent

final case class MetricsReset(
  timestamp: ZonedDateTime,
  serviceInfo: ServiceInfo,
  serviceParams: ServiceParams,
  prev: Option[ZonedDateTime],
  next: Option[ZonedDateTime],
  snapshot: MetricsSnapshot
) extends ServiceEvent

sealed trait ActionEvent extends NJEvent {
  def actionInfo: ActionInfo // action runtime information
  def actionParams: ActionParams // action static parameters
}

final case class ActionStart(actionParams: ActionParams, actionInfo: ActionInfo, timestamp: ZonedDateTime)
    extends ActionEvent

final case class ActionRetrying(
  actionParams: ActionParams,
  actionInfo: ActionInfo,
  timestamp: ZonedDateTime,
  willDelayAndRetry: WillDelayAndRetry,
  error: NJError)
    extends ActionEvent

final case class ActionFailed(
  actionParams: ActionParams,
  actionInfo: ActionInfo,
  timestamp: ZonedDateTime,
  numRetries: Int, // number of retries before giving up
  notes: Notes, // failure notes
  error: NJError)
    extends ActionEvent

final case class ActionSucced(
  actionParams: ActionParams,
  actionInfo: ActionInfo,
  timestamp: ZonedDateTime,
  numRetries: Int, // number of retries before success
  notes: Notes // success notes
) extends ActionEvent

final case class ActionQuasiSucced(
  actionParams: ActionParams,
  actionInfo: ActionInfo,
  timestamp: ZonedDateTime,
  runMode: RunMode,
  numSucc: Long, // succed actions
  succNotes: Notes,
  failNotes: Notes,
  errors: List[NJError])
    extends ActionEvent

final case class PassThrough(timestamp: ZonedDateTime, metricName: String, value: Json) extends NJEvent
