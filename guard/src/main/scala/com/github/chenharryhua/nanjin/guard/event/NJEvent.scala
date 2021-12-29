package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, DigestedName, Importance, ServiceParams}
import io.circe.generic.auto.*
import io.circe.shapes.*
import io.circe.{Decoder, Encoder, Json}
import retry.RetryDetails
import retry.RetryDetails.WillDelayAndRetry

import java.time.ZonedDateTime
import java.util.UUID

sealed trait NJEvent {
  def timestamp: ZonedDateTime // event timestamp - when the event occurs
  def serviceInfo: ServiceInfo
  def uuid: UUID
  def name: DigestedName
  final def show: String = NJEvent.showNJEvent.show(this)
  final def asJson: Json = NJEvent.encoderNJEvent.apply(this)
}

object NJEvent {
  implicit final val showNJEvent: Show[NJEvent]       = cats.derived.semiauto.show[NJEvent]
  implicit final val encoderNJEvent: Encoder[NJEvent] = io.circe.generic.semiauto.deriveEncoder[NJEvent]
  implicit final val decoderNJEvent: Decoder[NJEvent] = io.circe.generic.semiauto.deriveDecoder[NJEvent]
}

sealed trait ServiceEvent extends NJEvent {
  final override def uuid: UUID = serviceInfo.uuid

}

final case class ServiceStarted(serviceInfo: ServiceInfo, timestamp: ZonedDateTime) extends ServiceEvent {
  override val name: DigestedName = serviceInfo.serviceParams.name
}

final case class ServicePanic(
  serviceInfo: ServiceInfo,
  timestamp: ZonedDateTime,
  retryDetails: RetryDetails,
  error: NJError
) extends ServiceEvent {
  override val name: DigestedName = serviceInfo.serviceParams.name
}

final case class ServiceStopped(
  serviceInfo: ServiceInfo,
  timestamp: ZonedDateTime,
  snapshot: MetricsSnapshot
) extends ServiceEvent {
  override val name: DigestedName = serviceInfo.serviceParams.name
}

final case class ServiceAlert(
  name: DigestedName,
  serviceInfo: ServiceInfo,
  timestamp: ZonedDateTime,
  importance: Importance,
  message: String
) extends ServiceEvent

final case class MetricsReport(
  reportType: MetricReportType,
  serviceInfo: ServiceInfo,
  timestamp: ZonedDateTime,
  snapshot: MetricsSnapshot
) extends ServiceEvent {
  override val name: DigestedName = serviceInfo.serviceParams.name
}

final case class MetricsReset(
  resetType: MetricResetType,
  serviceInfo: ServiceInfo,
  timestamp: ZonedDateTime,
  snapshot: MetricsSnapshot
) extends ServiceEvent {
  override val name: DigestedName = serviceInfo.serviceParams.name
}

final case class PassThrough(
  name: DigestedName,
  asError: Boolean, // the payload json represent an error
  serviceInfo: ServiceInfo,
  timestamp: ZonedDateTime,
  value: Json
) extends ServiceEvent

sealed trait ActionEvent extends NJEvent {
  def actionInfo: ActionInfo // action runtime information
  final override def serviceInfo: ServiceInfo = actionInfo.serviceInfo
  final override def uuid: UUID               = actionInfo.uuid
  final override def name: DigestedName       = actionInfo.actionParams.name
  final def actionParams: ActionParams        = actionInfo.actionParams
  final def serviceParams: ServiceParams      = actionInfo.serviceParams
  final def launchTime: ZonedDateTime         = actionInfo.launchTime
}

final case class ActionStart(actionInfo: ActionInfo) extends ActionEvent {
  override val timestamp: ZonedDateTime = actionInfo.launchTime
}

final case class ActionRetrying(
  actionInfo: ActionInfo,
  timestamp: ZonedDateTime,
  willDelayAndRetry: WillDelayAndRetry,
  error: NJError)
    extends ActionEvent

final case class ActionFailed(
  actionInfo: ActionInfo,
  timestamp: ZonedDateTime,
  numRetries: Int, // number of retries before giving up
  notes: Notes, // failure notes
  error: NJError)
    extends ActionEvent

final case class ActionSucced(
  actionInfo: ActionInfo,
  timestamp: ZonedDateTime,
  numRetries: Int, // number of retries before success
  notes: Notes // success notes
) extends ActionEvent
