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
import org.typelevel.cats.time.instances.{localdatetime, zoneddatetime}

import java.time.temporal.ChronoUnit
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
  throwable: Throwable
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
    } yield NJError(id, msg, st, new Throwable("fake")) // can not reconstruct throwables.

  def apply(uuid: UUID, ex: Throwable): NJError =
    NJError(uuid, ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex), ex)
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
sealed trait ServiceStatus {
  def serviceParams: ServiceParams
  def isUp: Boolean
  def isDown: Boolean
  def isStopped: Boolean

  def goUp(now: ZonedDateTime): ServiceStatus
  def goDown(now: ZonedDateTime, upcomingDelay: Option[FiniteDuration], cause: String): ServiceStatus

  final def upTime(now: ZonedDateTime): Duration = Duration.between(serviceParams.launchTime, now)
  final def upTime(now: Instant): Duration       = Duration.between(serviceParams.launchTime, now)

  final def serviceID: UUID           = serviceParams.serviceID
  final def launchTime: ZonedDateTime = serviceParams.launchTime

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

object ServiceStatus extends zoneddatetime {
  implicit val showServiceStatus: Show[ServiceStatus] = cats.derived.semiauto.show[ServiceStatus]

  @JsonCodec
  final case class Up(serviceParams: ServiceParams, lastRestartAt: ZonedDateTime, lastCrashAt: ZonedDateTime)
      extends ServiceStatus {

    override def goUp(now: ZonedDateTime): Up = this.copy(lastRestartAt = now)
    override def goDown(now: ZonedDateTime, upcomingDelay: Option[FiniteDuration], cause: String): Down =
      Down(serviceParams, now, upcomingDelay.map(fd => now.plus(fd.toJava)), cause)

    override val isUp: Boolean      = true
    override val isDown: Boolean    = false
    override val isStopped: Boolean = false
  }

  object Up {
    def apply(serviceParams: ServiceParams): ServiceStatus =
      Up(serviceParams, serviceParams.launchTime, serviceParams.launchTime)
  }

  @JsonCodec
  final case class Down(
    serviceParams: ServiceParams,
    crashAt: ZonedDateTime,
    upcommingRestart: Option[ZonedDateTime],
    cause: String)
      extends ServiceStatus {

    override def goUp(now: ZonedDateTime): Up = Up(serviceParams, now, crashAt)
    override def goDown(now: ZonedDateTime, upcomingDelay: Option[FiniteDuration], cause: String): Down =
      this.copy(crashAt = now, upcommingRestart = upcomingDelay.map(fd => now.plus(fd.toJava)), cause = cause)

    override val isUp: Boolean      = false
    override val isDown: Boolean    = true
    override val isStopped: Boolean = upcommingRestart.isEmpty
  }
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
