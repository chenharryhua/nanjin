package com.github.chenharryhua.nanjin.guard.alert

import cats.Show
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, ServiceParams, Severity}
import io.chrisdavenport.cats.time.instances.{localtime, zoneddatetime, zoneid}
import io.circe.generic.auto.*
import io.circe.shapes.*
import io.circe.{Decoder, Encoder, Json}
import retry.RetryDetails
import retry.RetryDetails.WillDelayAndRetry

import java.time.ZonedDateTime

sealed trait NJEvent {
  def timestamp: ZonedDateTime // event timestamp - when the event occurs
  def severity: Severity
}

object NJEvent extends zoneddatetime with localtime with zoneid {
  implicit val showNJEvent: Show[NJEvent]       = cats.derived.semiauto.show[NJEvent]
  implicit val encoderNJEvent: Encoder[NJEvent] = io.circe.generic.semiauto.deriveEncoder[NJEvent]
  implicit val decoderNJEvent: Decoder[NJEvent] = io.circe.generic.semiauto.deriveDecoder[NJEvent]
}

sealed trait ServiceEvent extends NJEvent {
  def serviceInfo: ServiceInfo // service runtime infomation
  def serviceParams: ServiceParams // service static parameters
}

final case class ServiceStarted(timestamp: ZonedDateTime, serviceInfo: ServiceInfo, serviceParams: ServiceParams)
    extends ServiceEvent {
  override val severity: Severity = Severity.SystemEvent
}

final case class ServicePanic(
  timestamp: ZonedDateTime,
  serviceInfo: ServiceInfo,
  serviceParams: ServiceParams,
  retryDetails: RetryDetails,
  error: NJError
) extends ServiceEvent {
  override val severity: Severity = Severity.SystemEvent
}

final case class ServiceStopped(
  timestamp: ZonedDateTime,
  serviceInfo: ServiceInfo,
  serviceParams: ServiceParams
) extends ServiceEvent {
  override val severity: Severity = Severity.SystemEvent
}

final case class ServiceHealthCheck(
  timestamp: ZonedDateTime,
  serviceInfo: ServiceInfo,
  serviceParams: ServiceParams,
  dailySummaries: DailySummaries
) extends ServiceEvent {
  override val severity: Severity = Severity.SystemEvent
}

final case class ServiceDailySummariesReset(
  timestamp: ZonedDateTime,
  serviceInfo: ServiceInfo,
  serviceParams: ServiceParams,
  dailySummaries: DailySummaries)
    extends ServiceEvent {
  override val severity: Severity = Severity.SystemEvent
}

sealed trait ActionEvent extends NJEvent {
  def actionInfo: ActionInfo // action runtime information
  def actionParams: ActionParams // action static parameters
}

final case class ActionStart(
  timestamp: ZonedDateTime,
  severity: Severity,
  actionInfo: ActionInfo,
  actionParams: ActionParams)
    extends NJEvent

final case class ActionRetrying(
  timestamp: ZonedDateTime,
  actionInfo: ActionInfo,
  actionParams: ActionParams,
  willDelayAndRetry: WillDelayAndRetry,
  error: NJError
) extends ActionEvent {
  override val severity: Severity = error.severity
}

final case class ActionFailed(
  timestamp: ZonedDateTime,
  actionInfo: ActionInfo,
  actionParams: ActionParams,
  numRetries: Int, // number of retries before giving up
  notes: Notes, // failure notes
  error: NJError
) extends ActionEvent {
  override val severity: Severity = Severity.Critical
}

final case class ActionSucced(
  timestamp: ZonedDateTime,
  severity: Severity,
  actionInfo: ActionInfo,
  actionParams: ActionParams,
  numRetries: Int, // how many retries before success
  notes: Notes // success notes
) extends ActionEvent

final case class ActionQuasiSucced(
  timestamp: ZonedDateTime,
  severity: Severity,
  actionInfo: ActionInfo,
  actionParams: ActionParams,
  runMode: RunMode,
  numSucc: Long,
  succNotes: Notes,
  failNotes: Notes,
  errors: List[NJError]
) extends ActionEvent

final case class ForYourInformation(timestamp: ZonedDateTime, message: String) extends NJEvent {
  override val severity: Severity = Severity.Critical
}

final case class PassThrough(timestamp: ZonedDateTime, value: Json) extends NJEvent {
  override val severity: Severity = Severity.Critical
}
