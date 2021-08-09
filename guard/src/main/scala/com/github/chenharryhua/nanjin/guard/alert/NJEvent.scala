package com.github.chenharryhua.nanjin.guard.alert

import cats.Show
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, ServiceParams}
import enumeratum.*
import io.chrisdavenport.cats.time.instances.{localtime, zoneddatetime, zoneid}
import io.circe.generic.auto.*
import io.circe.shapes.*
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils
import retry.RetryDetails
import retry.RetryDetails.WillDelayAndRetry

import java.time.ZonedDateTime
import java.util.UUID
import scala.collection.JavaConverters.*
import scala.collection.immutable

final case class ServiceInfo(id: UUID, launchTime: ZonedDateTime)

final case class ActionInfo(id: UUID, launchTime: ZonedDateTime, actionName: String, serviceInfo: ServiceInfo)

final case class Notes private (value: String)

object Notes {
  def apply(str: String): Notes = new Notes(Option(str).getOrElse("null in notes"))
}

final case class NJError private (id: UUID, message: String, stackTrace: String, throwable: Throwable)

object NJError {
  implicit val showNJError: Show[NJError] = ex => s"NJError(id=${ex.id}, message=${ex.message})"

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

final case class DailySummaries private (value: String)

object DailySummaries {
  def apply(registry: MetricRegistry): DailySummaries = {
    val timer   = registry.getTimers.asScala.map { case (s, t) => s"$s: *${t.getCount}*" }.toList
    val counter = registry.getCounters.asScala.map { case (s, c) => s"$s: *${c.getCount}*" }.toList
    val all     = (timer ::: counter).sorted.mkString("\n")
    DailySummaries(all)
  }
}

sealed trait NJEvent {
  def timestamp: ZonedDateTime // event timestamp - when the event occurs
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

final case class ServiceHealthCheck(
  timestamp: ZonedDateTime,
  serviceInfo: ServiceInfo,
  serviceParams: ServiceParams,
  dailySummaries: DailySummaries
) extends ServiceEvent

final case class ServiceDailySummariesReset(
  timestamp: ZonedDateTime,
  serviceInfo: ServiceInfo,
  serviceParams: ServiceParams,
  dailySummaries: DailySummaries)
    extends ServiceEvent

sealed trait ActionEvent extends NJEvent {
  def actionInfo: ActionInfo // action runtime information
  def actionParams: ActionParams // action static parameters
}

final case class ActionStart(timestamp: ZonedDateTime, actionInfo: ActionInfo, actionParams: ActionParams)
    extends NJEvent

final case class ActionRetrying(
  timestamp: ZonedDateTime,
  actionInfo: ActionInfo,
  actionParams: ActionParams,
  willDelayAndRetry: WillDelayAndRetry,
  error: NJError
) extends ActionEvent

final case class ActionFailed(
  timestamp: ZonedDateTime,
  actionInfo: ActionInfo,
  actionParams: ActionParams,
  numRetries: Int, // number of retries before giving up
  notes: Notes, // failure notes
  error: NJError
) extends ActionEvent

final case class ActionSucced(
  timestamp: ZonedDateTime,
  actionInfo: ActionInfo,
  actionParams: ActionParams,
  numRetries: Int, // how many retries before success
  notes: Notes // success notes
) extends ActionEvent

sealed trait RunMode extends EnumEntry
object RunMode extends Enum[RunMode] with CatsEnum[RunMode] with CirceEnum[RunMode] {
  override val values: immutable.IndexedSeq[RunMode] = findValues
  case object Parallel extends RunMode
  case object Sequential extends RunMode
}

final case class ActionQuasiSucced(
  timestamp: ZonedDateTime,
  actionInfo: ActionInfo,
  actionParams: ActionParams,
  runMode: RunMode,
  numSucc: Long,
  succNotes: Notes,
  failNotes: Notes,
  errors: List[NJError]
) extends ActionEvent

final case class ForYourInformation(timestamp: ZonedDateTime, message: String, isError: Boolean) extends NJEvent

final case class PassThrough(timestamp: ZonedDateTime, value: Json) extends NJEvent
