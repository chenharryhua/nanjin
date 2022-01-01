package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.derived.auto.show.*
import cats.implicits.{catsSyntaxEq, toShow}
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, MetricSnapshotType, ServiceParams}
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import io.circe.shapes.*
import io.circe.syntax.*
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}
import java.util.UUID

@JsonCodec
sealed trait NJRuntimeInfo {
  def serviceParams: ServiceParams
  def uuid: UUID
  def launchTime: ZonedDateTime
}

final case class ServiceInfo(serviceParams: ServiceParams, uuid: UUID, launchTime: ZonedDateTime) extends NJRuntimeInfo
final case class ActionInfo(actionParams: ActionParams, serviceInfo: ServiceInfo, uuid: UUID, launchTime: ZonedDateTime)
    extends NJRuntimeInfo {
  override val serviceParams: ServiceParams = serviceInfo.serviceParams
}

@JsonCodec
final case class Notes private (value: String) extends AnyVal

private[guard] object Notes {
  def apply(str: String): Notes = new Notes(Option(str).getOrElse("null in notes"))
}

final case class NJError private (
  uuid: UUID,
  message: String,
  stackTrace: String,
  throwable: Option[Throwable]
)

private[guard] object NJError {
  implicit val showNJError: Show[NJError] = ex => s"NJError(id=${ex.uuid.show}, message=${ex.message})"

  implicit val encodeNJError: Encoder[NJError] = (a: NJError) =>
    Json.obj(
      ("uuid", a.uuid.asJson),
      ("message", a.message.asJson),
      ("stackTrace", a.stackTrace.asJson)
    )

  implicit val decodeNJError: Decoder[NJError] = (c: HCursor) =>
    for {
      id <- c.downField("uuid").as[UUID]
      msg <- c.downField("message").as[String]
      st <- c.downField("stackTrace").as[String]
    } yield NJError(id, msg, st, None) // can not reconstruct throwables.

  def apply(uuid: UUID, ex: Throwable): NJError =
    NJError(uuid, ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex), Some(ex))
}

@JsonCodec
sealed trait MetricResetType
object MetricResetType {
  implicit val showMetricResetType: Show[MetricResetType] = {
    case Adhoc           => s"Adhoc Metric Reset"
    case Scheduled(next) => s"Scheduled Metric Reset(next=${next.toLocalDateTime.truncatedTo(ChronoUnit.SECONDS).show})"
  }
  case object Adhoc extends MetricResetType
  final case class Scheduled(next: ZonedDateTime) extends MetricResetType
}

@JsonCodec
sealed trait MetricReportType {
  def isShow: Boolean
  def snapshotType: MetricSnapshotType
}

object MetricReportType {
  implicit val showMetricReportType: Show[MetricReportType] = {
    case Adhoc(mst)            => s"Adhoc ${mst.show} Metric Report"
    case Scheduled(index, mst) => s"Scheduled ${mst.show} Metric Report(index=$index)"
  }

  final case class Adhoc(snapshotType: MetricSnapshotType) extends MetricReportType {
    override val isShow: Boolean = true
  }

  final case class Scheduled(index: Long, snapshotType: MetricSnapshotType) extends MetricReportType {
    override val isShow: Boolean = index === 0
  }
}

@JsonCodec
sealed trait ServiceStatus {
  def serviceParams: ServiceParams
  def uuid: UUID
  def launchTime: ZonedDateTime
  def upTime(now: ZonedDateTime): Duration
  def isUp: Boolean

  final def flip(now: ZonedDateTime): ServiceStatus = this match {
    case up: ServiceStatus.Up     => up.down(now)
    case down: ServiceStatus.Down => down.up(now)
  }
}

object ServiceStatus {
  implicit val showServiceStatus: Show[ServiceStatus] = cats.derived.semiauto.show[ServiceStatus]

  @JsonCodec
  final case class Up private[ServiceStatus] (
    serviceParams: ServiceParams,
    uuid: UUID,
    launchTime: ZonedDateTime,
    lastRestartAt: ZonedDateTime,
    lastCrashAt: ZonedDateTime)
      extends ServiceStatus {
    override def upTime(now: ZonedDateTime): Duration = Duration.between(launchTime, now)
    override val isUp: Boolean                        = true
    def down(crashOn: ZonedDateTime): Down            = Down(serviceParams, uuid, launchTime, crashOn)
  }

  object Up {
    def apply(serviceParams: ServiceParams, uuid: UUID, launchTime: ZonedDateTime): Up =
      Up(serviceParams, uuid, launchTime, launchTime, launchTime)
  }

  @JsonCodec
  final case class Down private[ServiceStatus] (
    serviceParams: ServiceParams,
    uuid: UUID,
    launchTime: ZonedDateTime,
    crashAt: ZonedDateTime)
      extends ServiceStatus {
    override def upTime(now: ZonedDateTime): Duration = Duration.between(launchTime, now)
    override val isUp: Boolean                        = false
    def up(restart: ZonedDateTime): Up                = Up(serviceParams, uuid, launchTime, restart, crashAt)
  }
}
