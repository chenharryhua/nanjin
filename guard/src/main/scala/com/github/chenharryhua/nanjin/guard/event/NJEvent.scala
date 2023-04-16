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

  final case class ServiceStart(serviceParams: ServiceParams, timestamp: ZonedDateTime) extends NJEvent

  final case class ServicePanic(
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    restartTime: ZonedDateTime,
    error: NJError)
      extends NJEvent

  final case class ServiceStop(
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    cause: ServiceStopCause)
      extends NJEvent

  final case class ServiceAlert(
    metricName: MetricName,
    timestamp: ZonedDateTime,
    serviceParams: ServiceParams,
    alertLevel: AlertLevel,
    message: Json)
      extends NJEvent

  sealed trait MetricEvent extends NJEvent {
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

  sealed trait ActionEvent extends NJEvent {
    def actionInfo: ActionInfo // action runtime information
    def actionParams: ActionParams

    final override def serviceParams: ServiceParams = actionParams.serviceParams

    final def traceId: String    = actionInfo.traceId.getOrElse("null")
    final def actionId: String   = actionInfo.actionId
    final def metricId: MetricID = actionParams.metricId
  }

  final case class ActionStart(actionParams: ActionParams, actionInfo: ActionInfo, json: Json)
      extends ActionEvent {
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
    override def timestamp: ZonedDateTime = serviceParams.toZonedDateTime(landTime)
    def tookSoFar: Duration               = actionInfo.took(landTime)
  }

  sealed trait ActionResultEvent extends ActionEvent {
    def landTime: FiniteDuration
    def json: Json

    final override def timestamp: ZonedDateTime = serviceParams.toZonedDateTime(landTime)
    final def took: Duration                    = actionInfo.took(landTime)
  }

  @Lenses
  final case class ActionFail(
    actionParams: ActionParams,
    actionInfo: ActionInfo,
    landTime: FiniteDuration,
    error: NJError,
    json: Json)
      extends ActionResultEvent

  @Lenses
  final case class ActionComplete(
    actionParams: ActionParams,
    actionInfo: ActionInfo,
    landTime: FiniteDuration,
    json: Json)
      extends ActionResultEvent

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
