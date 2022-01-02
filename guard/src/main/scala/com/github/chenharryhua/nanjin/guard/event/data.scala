package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.derived.auto.show.*
import cats.implicits.{catsSyntaxEq, toShow}
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, DigestedName, MetricSnapshotType, ServiceParams}
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import io.circe.shapes.*
import io.circe.syntax.*
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}
import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

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
final case class PendingAction private (name: DigestedName, uuid: UUID, launchTime: ZonedDateTime)
object PendingAction {
  implicit val showPendingAction: Show[PendingAction] = cats.derived.semiauto.show[PendingAction]
  def apply(ai: ActionInfo): PendingAction =
    PendingAction(
      ai.actionParams.name,
      ai.uuid,
      ai.launchTime
    )
}

@JsonCodec
final case class ActionInfo(
  actionParams: ActionParams,
  serviceStatus: ServiceStatus,
  serviceParams: ServiceParams,
  uuid: UUID,
  launchTime: ZonedDateTime)

object ActionInfo {
  implicit val showActionInfo: Show[ActionInfo] = cats.derived.semiauto.show[ActionInfo]
}

@JsonCodec
sealed trait ServiceStatus {
  def uuid: UUID
  def launchTime: ZonedDateTime
  def upTime(now: ZonedDateTime): Duration
  def isUp: Boolean
  def isDown: Boolean
  def goUp(now: ZonedDateTime): ServiceStatus.Up
  def goDown(now: ZonedDateTime, upcomingDelay: Option[FiniteDuration]): ServiceStatus.Down

  final def fold[A](up: ServiceStatus.Up => A)(down: ServiceStatus.Down => A): A =
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

object ServiceStatus {
  implicit val showServiceStatus: Show[ServiceStatus] = cats.derived.semiauto.show[ServiceStatus]

  @JsonCodec
  final case class Up private[ServiceStatus] (
    uuid: UUID,
    launchTime: ZonedDateTime,
    lastRestartAt: ZonedDateTime,
    lastCrashAt: ZonedDateTime)
      extends ServiceStatus {
    override def upTime(now: ZonedDateTime): Duration = Duration.between(launchTime, now)

    override def goUp(now: ZonedDateTime): Up = this
    override def goDown(now: ZonedDateTime, upcomingDelay: Option[FiniteDuration]): Down =
      Down(uuid, launchTime, now, upcomingDelay.map(fd => now.plus(fd.toJava)))

    override val isUp: Boolean   = true
    override val isDown: Boolean = false
  }

  object Up {
    def apply(uuid: UUID, launchTime: ZonedDateTime): ServiceStatus = Up(uuid, launchTime, launchTime, launchTime)
  }

  @JsonCodec
  final case class Down private[ServiceStatus] (
    uuid: UUID,
    launchTime: ZonedDateTime,
    crashAt: ZonedDateTime,
    upcommingRestart: Option[ZonedDateTime])
      extends ServiceStatus {
    override def upTime(now: ZonedDateTime): Duration = Duration.between(launchTime, now)

    override def goUp(now: ZonedDateTime): Up = Up(uuid, launchTime, now, crashAt)
    override def goDown(now: ZonedDateTime, upcomingDelay: Option[FiniteDuration]): Down = this

    override val isUp: Boolean   = false
    override val isDown: Boolean = true
  }
}
