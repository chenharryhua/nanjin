package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.derived.auto.show.*
import cats.kernel.Eq
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.*
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import io.circe.shapes.*
import io.circe.syntax.*
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.typelevel.cats.time.instances.instant

import java.time.{Duration, Instant, ZonedDateTime}
import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

@JsonCodec
final case class Notes private (value: String) extends AnyVal

private[guard] object Notes {
  def apply(str: String): Notes = new Notes(Option(str).getOrElse("null in notes"))
  val empty: Notes              = Notes("")
}

final case class NJError private (
  uuid: UUID,
  message: String,
  stackTrace: String,
  throwable: Option[Throwable]
)

private[guard] object NJError {
  implicit val showNJError: Show[NJError] = ex => s"NJError(id=${ex.uuid.show}, message=${ex.message})"
  implicit val eqNJError: Eq[NJError]     = (x: NJError, y: NJError) => x.uuid === y.uuid

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
    case Scheduled(next) => s"Scheduled Metric Reset(next=$next)"
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
final case class OngoingAction private (metricName: DigestedName, uniqueId: Int, launchTime: Instant)
object OngoingAction extends instant {
  implicit val showPendingAction: Show[OngoingAction] = cats.derived.semiauto.show[OngoingAction]
  def apply(ai: ActionInfo): OngoingAction =
    OngoingAction(
      ai.actionParams.metricName,
      ai.uniqueId,
      ai.launchTime
    )
}

@JsonCodec
final case class ActionInfo(actionParams: ActionParams, uniqueId: Int, launchTime: Instant) {
  val isCritical: Boolean  = actionParams.importance > Importance.High // Critical
  val isNotice: Boolean    = actionParams.importance > Importance.Medium // Hight + Critical
  val nonTrivial: Boolean  = actionParams.importance > Importance.Low // Medium + High + Critical
  val isExpensive: Boolean = actionParams.isExpensive.value
}

object ActionInfo extends instant {
  implicit val showActionInfo: Show[ActionInfo] = cats.derived.semiauto.show[ActionInfo]
}

@JsonCodec
sealed trait ServiceStatus {
  def uuid: UUID
  def launchTime: Instant
  def isUp: Boolean
  def isDown: Boolean
  def isStopped: Boolean

  def goUp(now: Instant): ServiceStatus
  def goDown(now: Instant, upcomingDelay: Option[FiniteDuration], cause: String): ServiceStatus

  final def upTime(now: Instant): Duration = Duration.between(launchTime, now)
  final def fold[A](up: ServiceStatus.Up => A, down: ServiceStatus.Down => A): A =
    this match {
      case s: ServiceStatus.Up   => up(s)
      case s: ServiceStatus.Down => down(s)
    }
}

/** Up - service is up
  *
  * Down: Stopped when upcommingRestart is None
  *
  * : restarting when upcommingRestart is Some
  */

object ServiceStatus extends instant {
  implicit val showServiceStatus: Show[ServiceStatus] = cats.derived.semiauto.show[ServiceStatus]

  @JsonCodec
  final case class Up(uuid: UUID, launchTime: Instant, lastRestartAt: Instant, lastCrashAt: Instant)
      extends ServiceStatus {

    override def goUp(now: Instant): Up = this
    override def goDown(now: Instant, upcomingDelay: Option[FiniteDuration], cause: String): Down =
      Down(uuid, launchTime, now, upcomingDelay.map(fd => now.plus(fd.toJava)), cause)

    override val isUp: Boolean      = true
    override val isDown: Boolean    = false
    override val isStopped: Boolean = false
  }

  object Up {
    def apply(uuid: UUID, launchTime: Instant): ServiceStatus = Up(uuid, launchTime, launchTime, launchTime)
  }

  @JsonCodec
  final case class Down(
    uuid: UUID,
    launchTime: Instant,
    crashAt: Instant,
    upcommingRestart: Option[Instant],
    cause: String)
      extends ServiceStatus {

    override def goUp(now: Instant): Up = Up(uuid, launchTime, now, crashAt)
    override def goDown(now: Instant, upcomingDelay: Option[FiniteDuration], cause: String): Down = this

    override val isUp: Boolean      = false
    override val isDown: Boolean    = true
    override val isStopped: Boolean = upcommingRestart.isEmpty
  }
}

@JsonCodec
sealed trait ServiceStopCause
object ServiceStopCause {
  implicit val showServiceStopCause: Show[ServiceStopCause] = cats.derived.semiauto.show[ServiceStopCause]
  case object Normally extends ServiceStopCause
  final case class Abnormally(msg: String) extends ServiceStopCause
}
