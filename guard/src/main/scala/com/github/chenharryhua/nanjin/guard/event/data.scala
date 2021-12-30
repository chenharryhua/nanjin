package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.implicits.{catsSyntaxEq, toShow}
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, MetricSnapshotType, ServiceParams}
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import io.circe.shapes.*
import io.circe.syntax.*
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.ZonedDateTime
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
    case Scheduled(next) => s"Scheduled Metric Reset(next=${next.show})"
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
    case Adhoc(mst)          => s"Adhoc ${mst.show} Metric Report"
    case Scheduled(index, _) => s"Scheduled Metric Report(index=$index)"
  }

  final case class Adhoc(snapshotType: MetricSnapshotType) extends MetricReportType {
    override val isShow: Boolean = true
  }

  final case class Scheduled(index: Long, snapshotType: MetricSnapshotType) extends MetricReportType {
    override val isShow: Boolean = index === 0
  }
}
