package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.derived.auto.show.*
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Digested, Importance, ServiceParams}
import io.circe.generic.auto.*
import io.circe.shapes.*
import io.circe.{Decoder, Encoder, Json}
import retry.RetryDetails
import retry.RetryDetails.WillDelayAndRetry

import java.time.{Duration, ZoneId, ZonedDateTime}
import java.util.UUID

sealed trait NJEvent {
  def timestamp: ZonedDateTime // event timestamp - when the event occurs
  def serviceParams: ServiceParams

  final def zoneId: ZoneId   = serviceParams.taskParams.zoneId
  final def show: String     = NJEvent.showNJEvent.show(this)
  final def asJson: Json     = NJEvent.encoderNJEvent.apply(this)
  final def serviceID: UUID  = serviceParams.serviceID
  final def upTime: Duration = serviceParams.upTime(timestamp)
}

object NJEvent {
  implicit final val showNJEvent: Show[NJEvent]       = cats.derived.semiauto.show[NJEvent]
  implicit final val encoderNJEvent: Encoder[NJEvent] = io.circe.generic.semiauto.deriveEncoder[NJEvent]
  implicit final val decoderNJEvent: Decoder[NJEvent] = io.circe.generic.semiauto.deriveDecoder[NJEvent]
}

sealed trait ServiceEvent extends NJEvent {
  def isUp: Boolean
}

final case class ServiceStart(serviceParams: ServiceParams, timestamp: ZonedDateTime) extends ServiceEvent {
  override val isUp: Boolean = true
}

final case class ServicePanic(
  serviceParams: ServiceParams,
  timestamp: ZonedDateTime,
  retryDetails: RetryDetails,
  error: NJError
) extends ServiceEvent {
  override val isUp: Boolean = false
}

final case class ServiceStop(
  serviceParams: ServiceParams,
  timestamp: ZonedDateTime,
  cause: ServiceStopCause
) extends ServiceEvent {
  override val isUp: Boolean = false
}

final case class MetricReport(
  reportType: MetricReportType,
  serviceParams: ServiceParams,
  ongoings: List[ActionInfo],
  timestamp: ZonedDateTime,
  snapshot: MetricSnapshot,
  isUp: Boolean
) extends ServiceEvent {

  val hasError: Boolean = snapshot.isContainErrors // || serviceParams.isDown
}

final case class MetricReset(
  resetType: MetricResetType,
  serviceParams: ServiceParams,
  timestamp: ZonedDateTime,
  snapshot: MetricSnapshot,
  isUp: Boolean
) extends ServiceEvent {
  val hasError: Boolean = snapshot.isContainErrors // || serviceParams.isDown
}

sealed trait ActionEvent extends NJEvent {
  def actionInfo: ActionInfo // action runtime information

  final override def serviceParams: ServiceParams = actionInfo.actionParams.serviceParams

  final def metricName: Digested       = actionInfo.actionParams.metricName
  final def actionParams: ActionParams = actionInfo.actionParams
  final def launchTime: ZonedDateTime  = actionInfo.launchTime
  final def actionID: Int              = actionInfo.actionID

  final def took: Duration = Duration.between(actionInfo.launchTime, timestamp)
}

final case class ActionStart(actionInfo: ActionInfo) extends ActionEvent {
  override val timestamp: ZonedDateTime = actionInfo.launchTime
}

final case class ActionRetry(
  actionInfo: ActionInfo,
  timestamp: ZonedDateTime,
  willDelayAndRetry: WillDelayAndRetry,
  error: NJError)
    extends ActionEvent

final case class ActionFail(
  actionInfo: ActionInfo,
  timestamp: ZonedDateTime,
  numRetries: Int, // number of retries before giving up
  notes: Notes, // failure notes
  error: NJError)
    extends ActionEvent

final case class ActionSucc(
  actionInfo: ActionInfo,
  timestamp: ZonedDateTime,
  numRetries: Int, // number of retries before success
  notes: Notes // success notes
) extends ActionEvent

sealed trait InstantEvent extends NJEvent {
  def metricName: Digested
  def timestamp: ZonedDateTime
  def serviceParams: ServiceParams
}

final case class InstantAlert(
  metricName: Digested,
  timestamp: ZonedDateTime,
  serviceParams: ServiceParams,
  importance: Importance,
  message: String
) extends InstantEvent

final case class PassThrough(
  metricName: Digested,
  timestamp: ZonedDateTime,
  serviceParams: ServiceParams,
  asError: Boolean, // the payload json represent an error
  value: Json
) extends InstantEvent
