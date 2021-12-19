package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.guard.config.{Importance, MetricName}
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
  implicit final val showNJEvent: Show[NJEvent]       = cats.derived.semiauto.show[NJEvent]
  implicit final val encoderNJEvent: Encoder[NJEvent] = io.circe.generic.semiauto.deriveEncoder[NJEvent]
  implicit final val decoderNJEvent: Decoder[NJEvent] = io.circe.generic.semiauto.deriveDecoder[NJEvent]
}

sealed trait ServiceEvent extends NJEvent {
  def serviceInfo: ServiceInfo // service runtime infomation
}

final case class ServiceStarted(serviceInfo: ServiceInfo, timestamp: ZonedDateTime) extends ServiceEvent

final case class ServicePanic(
  serviceInfo: ServiceInfo,
  timestamp: ZonedDateTime,
  retryDetails: RetryDetails,
  error: NJError
) extends ServiceEvent

final case class ServiceStopped(
  serviceInfo: ServiceInfo,
  timestamp: ZonedDateTime,
  snapshot: MetricsSnapshot
) extends ServiceEvent

final case class MetricsReport(
  reportType: MetricReportType,
  serviceInfo: ServiceInfo,
  timestamp: ZonedDateTime,
  snapshot: MetricsSnapshot
) extends ServiceEvent

final case class MetricsReset(
  resetType: MetricResetType,
  serviceInfo: ServiceInfo,
  timestamp: ZonedDateTime,
  snapshot: MetricsSnapshot
) extends ServiceEvent

final case class ServiceAlert(
  metricName: MetricName,
  serviceInfo: ServiceInfo,
  timestamp: ZonedDateTime,
  importance: Importance,
  message: String
) extends ServiceEvent

final case class PassThrough(metricName: MetricName, serviceInfo: ServiceInfo, timestamp: ZonedDateTime, value: Json)
    extends ServiceEvent

sealed trait ActionEvent extends NJEvent {
  def actionInfo: ActionInfo // action runtime information
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
