package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.common.guard.ServiceName
import com.github.chenharryhua.nanjin.guard.config.{
  ActionParams,
  AlertLevel,
  MetricID,
  MetricName,
  ServiceParams
}
import io.circe.Json
import io.circe.generic.JsonCodec
import monocle.macros.Lenses
import org.typelevel.cats.time.instances.zoneddatetime

import java.time.{Duration, ZonedDateTime}
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

@JsonCodec
sealed trait NJEvent extends Product with Serializable {
  def timestamp: ZonedDateTime // event timestamp - when the event occurs
  def serviceParams: ServiceParams

  final def serviceId: UUID          = serviceParams.serviceId
  final def serviceName: ServiceName = serviceParams.serviceName
  final def upTime: Duration         = serviceParams.upTime(timestamp)
}

object NJEvent extends zoneddatetime {
  implicit final val showNJEvent: Show[NJEvent] = cats.derived.semiauto.show[NJEvent]

  sealed trait ServiceEvent extends NJEvent

  final case class ServiceStart(serviceParams: ServiceParams, timestamp: ZonedDateTime) extends ServiceEvent

  final case class ServicePanic(
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    restartTime: ZonedDateTime,
    error: NJError)
      extends ServiceEvent

  final case class ServiceStop(
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    cause: ServiceStopCause)
      extends ServiceEvent

  sealed trait MetricEvent extends ServiceEvent {
    def index: MetricIndex
    def snapshot: MetricSnapshot
  }

  final case class MetricReport(
    index: MetricIndex,
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    snapshot: MetricSnapshot)
      extends MetricEvent

  final case class MetricReset(
    index: MetricIndex,
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    snapshot: MetricSnapshot)
      extends MetricEvent

  final case class InstantAlert(
    metricName: MetricName,
    timestamp: ZonedDateTime,
    serviceParams: ServiceParams,
    alertLevel: AlertLevel,
    message: String)
      extends ServiceEvent

  sealed trait ActionEvent extends ServiceEvent {
    def actionInfo: ActionInfo // action runtime information
    def actionParams: ActionParams

    final override def serviceParams: ServiceParams = actionParams.serviceParams

    final def traceId: String    = actionInfo.traceInfo.map(_.traceId).getOrElse("none")
    final def metricId: MetricID = actionParams.metricId
    final def actionId: String   = actionInfo.actionId
  }

  final case class ActionStart(actionParams: ActionParams, actionInfo: ActionInfo) extends ActionEvent {
    override def timestamp: ZonedDateTime = serviceParams.toZonedDateTime(actionInfo.launchTime)
  }

  final case class ActionRetry(
    actionParams: ActionParams,
    actionInfo: ActionInfo,
    landTime: FiniteDuration,
    retriesSoFar: Int,
    delay: FiniteDuration,
    error: NJError)
      extends ActionEvent {
    override val timestamp: ZonedDateTime = serviceParams.toZonedDateTime(landTime)
    val tookSoFar: Duration               = actionInfo.took(landTime)
  }

  sealed trait ActionResultEvent extends ActionEvent {
    def landTime: FiniteDuration
    final override def timestamp: ZonedDateTime = serviceParams.toZonedDateTime(landTime)
    final def took: Duration                    = actionInfo.took(landTime)
    def output: Json
  }

  @Lenses
  final case class ActionFail(
    actionParams: ActionParams,
    actionInfo: ActionInfo,
    landTime: FiniteDuration,
    error: NJError,
    output: Json)
      extends ActionResultEvent

  @Lenses
  final case class ActionComplete(
    actionParams: ActionParams,
    actionInfo: ActionInfo,
    landTime: FiniteDuration,
    output: Json // output of the action
  ) extends ActionResultEvent

  final def isPivotalEvent(evt: NJEvent): Boolean = evt match {
    case _: ActionComplete => false
    case _: ActionStart    => false
    case _                 => true
  }

  final def isActionDone(evt: ActionResultEvent): Boolean = evt match {
    case _: ActionFail     => false
    case _: ActionComplete => true
  }
}
