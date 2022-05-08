package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.derived.auto.show.*
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.*
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import io.circe.shapes.*
import org.apache.commons.lang3.exception.ExceptionUtils
import org.typelevel.cats.time.instances.{localdatetime, zoneddatetime}

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.util.UUID

@JsonCodec
final case class Notes private (value: String) extends AnyVal

private[guard] object Notes {
  def apply(str: String): Notes = new Notes(Option(str).getOrElse("null in notes"))
  val empty: Notes              = Notes("")
}

@JsonCodec
final case class NJError private (uuid: UUID, message: String, stackTrace: String)

private[guard] object NJError {
  implicit val showNJError: Show[NJError] = cats.derived.semiauto.show[NJError]
  def apply(uuid: UUID, ex: Throwable): NJError =
    NJError(uuid, Option(ex.getMessage).getOrElse(""), ExceptionUtils.getStackTrace(ex))
}

@JsonCodec
sealed trait MetricResetType
object MetricResetType extends localdatetime {
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
    case Scheduled(mst, index) => s"Scheduled ${mst.show} Metric Report(index=$index)"
  }

  final case class Adhoc(snapshotType: MetricSnapshotType) extends MetricReportType {
    override val isShow: Boolean = true
  }

  final case class Scheduled(snapshotType: MetricSnapshotType, index: Long) extends MetricReportType {
    override val isShow: Boolean = index === 0
  }
}

@JsonCodec
final case class ActionInfo(actionParams: ActionParams, actionID: Int, launchTime: ZonedDateTime)

object ActionInfo extends zoneddatetime {
  implicit val showActionInfo: Show[ActionInfo] = cats.derived.semiauto.show[ActionInfo]
}

@JsonCodec
sealed trait ServiceStopCause
object ServiceStopCause {
  implicit val showServiceStopCause: Show[ServiceStopCause] = {
    case Normally        => "normally exit"
    case Abnormally(msg) => s"abnormally exit due to $msg"
  }
  case object Normally extends ServiceStopCause
  final case class Abnormally(msg: String) extends ServiceStopCause
}
