package com.github.chenharryhua.nanjin.guard.event

import cats.{Monad, Show}
import cats.effect.kernel.Outcome
import cats.effect.kernel.Resource.ExitCase
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.*
import io.circe.generic.*
import natchez.{Span, Trace}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.typelevel.cats.time.instances.{localdatetime, zoneddatetime}
import java.net.URI
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

@JsonCodec
final case class NJError private (message: String, stackTrace: String)

private[guard] object NJError {
  implicit final val showNJError: Show[NJError] = cats.derived.semiauto.show[NJError]
  def apply(ex: Throwable): NJError =
    NJError(ExceptionUtils.getRootCauseMessage(ex), ExceptionUtils.getStackTrace(ex))
}

@JsonCodec
sealed trait MetricResetType extends Product with Serializable
object MetricResetType extends localdatetime {
  implicit final val showMetricResetType: Show[MetricResetType] = {
    case Adhoc => s"Adhoc Metric Reset"
    case Scheduled(next) =>
      s"Scheduled Metric Reset(next=${next.truncatedTo(ChronoUnit.SECONDS).toLocalDateTime.show})"
  }
  case object Adhoc extends MetricResetType
  final case class Scheduled(next: ZonedDateTime) extends MetricResetType
}

@JsonCodec
sealed trait MetricReportType extends Product with Serializable {
  def isShow: Boolean
  def snapshotType: MetricSnapshotType

  final def idx: Option[Long] = this match {
    case MetricReportType.Adhoc(_)            => None
    case MetricReportType.Scheduled(_, index) => Some(index)
  }
}

object MetricReportType {
  implicit final val showMetricReportType: Show[MetricReportType] = {
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
final case class ActionInfo(
  actionParams: ActionParams,
  actionId: Int,
  traceInfo: Option[TraceInfo],
  launchTime: ZonedDateTime)

object ActionInfo extends zoneddatetime {
  implicit final val showActionInfo: Show[ActionInfo] = cats.derived.semiauto.show[ActionInfo]
}

@JsonCodec
sealed trait ServiceStopCause extends Product with Serializable {
  def exitCode: Int

  final override def toString: String = this match {
    case ServiceStopCause.Normally         => "normally exit"
    case ServiceStopCause.ByCancelation    => "abnormally exit due to cancelation"
    case ServiceStopCause.ByException(msg) => s"abnormally exit due to $msg"
  }
}

object ServiceStopCause {
  def apply(ec: ExitCase): ServiceStopCause = ec match {
    case ExitCase.Succeeded  => ServiceStopCause.Normally
    case ExitCase.Errored(e) => ServiceStopCause.ByException(ExceptionUtils.getRootCauseMessage(e))
    case ExitCase.Canceled   => ServiceStopCause.ByCancelation
  }

  def apply[F[_], A](oc: Outcome[F, Throwable, A]): ServiceStopCause = oc match {
    case Outcome.Succeeded(_) => ServiceStopCause.Normally
    case Outcome.Errored(e)   => ServiceStopCause.ByException(ExceptionUtils.getRootCauseMessage(e))
    case Outcome.Canceled()   => ServiceStopCause.ByCancelation
  }

  implicit final val showServiceStopCause: Show[ServiceStopCause] = _.toString

  case object Normally extends ServiceStopCause {
    override val exitCode: Int = 0
  }

  case object ByCancelation extends ServiceStopCause {
    override val exitCode: Int = 1
  }

  final case class ByException(msg: String) extends ServiceStopCause {
    override val exitCode: Int = 2
  }
}

@JsonCodec
final case class TraceInfo(kernel: Map[String, String], traceId: Option[String], traceUri: Option[URI])
object TraceInfo {
  implicit final val showURI: Show[URI]             = _.toString
  implicit final val showTraceInfo: Show[TraceInfo] = cats.derived.semiauto.show

  def apply[F[_]: Monad](trace: Trace[F]): F[TraceInfo] = for {
    k <- trace.kernel.map(_.toHeaders)
    id <- trace.traceId
    uri <- trace.traceUri
  } yield TraceInfo(k, id, uri)

  def apply[F[_]: Monad](span: Span[F]): F[TraceInfo] = for {
    k <- span.kernel.map(_.toHeaders)
    id <- span.traceId
    uri <- span.traceUri
  } yield TraceInfo(k, id, uri)
}
