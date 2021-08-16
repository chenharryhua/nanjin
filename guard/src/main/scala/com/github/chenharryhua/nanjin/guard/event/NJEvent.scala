package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, ServiceParams, ThroughputLevel}
import io.chrisdavenport.cats.time.instances.{localtime, zoneddatetime, zoneid}
import io.circe.generic.auto.*
import io.circe.shapes.*
import io.circe.{Decoder, Encoder, Json}
import retry.RetryDetails
import retry.RetryDetails.WillDelayAndRetry

import java.time.{Duration, ZonedDateTime}
import scala.compat.java8.DurationConverters.*
import scala.concurrent.duration.FiniteDuration

sealed trait NJEvent {
  def timestamp: ZonedDateTime // event timestamp - when the event occurs
  def importance: Importance
}

object NJEvent extends zoneddatetime with localtime with zoneid {
  implicit private val finiteDurationEncoder: Encoder[FiniteDuration] = Encoder[Duration].contramap(_.toJava)
  implicit private val finiteDurationDecoder: Decoder[FiniteDuration] = Decoder[Duration].map(_.toScala)

  implicit val showNJEvent: Show[NJEvent]       = cats.derived.semiauto.show[NJEvent]
  implicit val encoderNJEvent: Encoder[NJEvent] = io.circe.generic.semiauto.deriveEncoder[NJEvent]
  implicit val decoderNJEvent: Decoder[NJEvent] = io.circe.generic.semiauto.deriveDecoder[NJEvent]
}

sealed trait ServiceEvent extends NJEvent {
  def serviceInfo: ServiceInfo // service runtime infomation
  def serviceParams: ServiceParams // service static parameters
  final override val importance: Importance = Importance.SystemEvent
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
  serviceParams: ServiceParams
) extends ServiceEvent

final case class MetricsReport(
  timestamp: ZonedDateTime,
  serviceInfo: ServiceInfo,
  serviceParams: ServiceParams,
  metrics: MetricRegistryWrapper
) extends ServiceEvent

sealed trait ActionEvent extends NJEvent {
  def actionInfo: ActionInfo // action runtime information
  def actionParams: ActionParams // action static parameters
  final override def importance: Importance = actionParams.throughputLevel match {
    case ThroughputLevel.High   => Importance.High
    case ThroughputLevel.Medium => Importance.Medium
    case ThroughputLevel.Low    => Importance.Low
  }
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
  numSucc: Long,
  succNotes: Notes,
  failNotes: Notes,
  errors: List[NJError])
    extends ActionEvent

final case class ForYourInformation(timestamp: ZonedDateTime, message: String) extends NJEvent {
  override val importance: Importance = Importance.High
}

final case class PassThrough(timestamp: ZonedDateTime, value: Json) extends NJEvent {
  override val importance: Importance = Importance.High
}
