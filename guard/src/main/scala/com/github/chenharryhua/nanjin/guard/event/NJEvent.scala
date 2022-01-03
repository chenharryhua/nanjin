package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.derived.auto.show.*
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, DigestedName, Importance, ServiceParams}
import io.circe.generic.auto.*
import io.circe.shapes.*
import io.circe.{Decoder, Encoder, Json}
import retry.RetryDetails
import retry.RetryDetails.WillDelayAndRetry

import java.time.{Duration, ZonedDateTime}
import java.util.UUID

sealed trait NJEvent {
  def timestamp: ZonedDateTime // event timestamp - when the event occurs
  def serviceStatus: ServiceStatus
  def serviceParams: ServiceParams
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
  final override def uuid: UUID = serviceStatus.uuid

  final def upTime: Duration = Duration.between(serviceStatus.launchTime, timestamp)
}

final case class ServiceStart(serviceStatus: ServiceStatus, timestamp: ZonedDateTime, serviceParams: ServiceParams)
    extends ServiceEvent {
  override val name: DigestedName = serviceParams.name

}

final case class ServicePanic(
  serviceStatus: ServiceStatus,
  timestamp: ZonedDateTime,
  retryDetails: RetryDetails,
  serviceParams: ServiceParams,
  error: NJError
) extends ServiceEvent {
  override val name: DigestedName = serviceParams.name
}

final case class ServiceStop(
  serviceStatus: ServiceStatus,
  timestamp: ZonedDateTime,
  serviceParams: ServiceParams,
  snapshot: MetricSnapshot
) extends ServiceEvent {
  override val name: DigestedName = serviceParams.name
}

final case class ServiceAlert(
  name: DigestedName,
  serviceStatus: ServiceStatus,
  timestamp: ZonedDateTime,
  importance: Importance,
  serviceParams: ServiceParams,
  message: String
) extends ServiceEvent

final case class MetricsReport(
  reportType: MetricReportType,
  serviceStatus: ServiceStatus,
  ongoings: List[OngoingAction],
  timestamp: ZonedDateTime,
  serviceParams: ServiceParams,
  snapshot: MetricSnapshot
) extends ServiceEvent {
  override val name: DigestedName = serviceParams.name

}

final case class MetricsReset(
  resetType: MetricResetType,
  serviceStatus: ServiceStatus,
  timestamp: ZonedDateTime,
  serviceParams: ServiceParams,
  snapshot: MetricSnapshot
) extends ServiceEvent {
  override val name: DigestedName = serviceParams.name
}

final case class PassThrough(
  name: DigestedName,
  asError: Boolean, // the payload json represent an error
  serviceStatus: ServiceStatus,
  timestamp: ZonedDateTime,
  serviceParams: ServiceParams,
  value: Json
) extends ServiceEvent

sealed trait ActionEvent extends NJEvent {
  def actionInfo: ActionInfo // action runtime information
  final override def serviceStatus: ServiceStatus = actionInfo.serviceStatus
  final override def serviceParams: ServiceParams = actionInfo.serviceParams
  final override def uuid: UUID                   = actionInfo.uuid
  final override def name: DigestedName           = actionInfo.actionParams.name
  final def actionParams: ActionParams            = actionInfo.actionParams
  final def launchTime: ZonedDateTime             = actionInfo.launchTime
}

final case class ActionStart(actionInfo: ActionInfo) extends ActionEvent {
  override val timestamp: ZonedDateTime = actionInfo.launchTime
}

final case class ActionRetry(
  actionInfo: ActionInfo,
  timestamp: ZonedDateTime,
  willDelayAndRetry: WillDelayAndRetry,
  error: NJError)
    extends ActionEvent {
  def took: Duration = Duration.between(actionInfo.launchTime, timestamp)
}

final case class ActionFail(
  actionInfo: ActionInfo,
  timestamp: ZonedDateTime,
  numRetries: Int, // number of retries before giving up
  notes: Notes, // failure notes
  error: NJError)
    extends ActionEvent {
  def took: Duration = Duration.between(actionInfo.launchTime, timestamp)
}

final case class ActionSucc(
  actionInfo: ActionInfo,
  timestamp: ZonedDateTime,
  numRetries: Int, // number of retries before success
  notes: Notes // success notes
) extends ActionEvent {
  def took: Duration = Duration.between(actionInfo.launchTime, timestamp)
}
