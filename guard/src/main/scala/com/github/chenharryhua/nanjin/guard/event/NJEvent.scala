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
  def title: String
  def isPivotal: Boolean // exclude ActionStart and ActionComplete event

  final def serviceId: UUID          = serviceParams.serviceId
  final def serviceName: ServiceName = serviceParams.serviceName
  final def upTime: Duration         = serviceParams.upTime(timestamp)
}

object NJEvent extends zoneddatetime {
  implicit final val showNJEvent: Show[NJEvent] = cats.derived.semiauto.show[NJEvent]

  sealed trait ServiceEvent extends NJEvent

  final case class ServiceStart(serviceParams: ServiceParams, timestamp: ZonedDateTime) extends ServiceEvent {
    override val title: String      = titles.serviceStart
    override val isPivotal: Boolean = true
  }

  final case class ServicePanic(
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    restartTime: ZonedDateTime,
    error: NJError)
      extends ServiceEvent {
    override val title: String      = titles.servicePanic
    override val isPivotal: Boolean = true
  }

  final case class ServiceStop(
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    cause: ServiceStopCause)
      extends ServiceEvent {
    override val title: String      = titles.serviceStop
    override val isPivotal: Boolean = true
  }

  sealed trait MetricEvent extends ServiceEvent {
    def index: MetricIndex
    def snapshot: MetricSnapshot
  }

  final case class MetricReport(
    index: MetricIndex,
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    snapshot: MetricSnapshot)
      extends MetricEvent {
    override val title: String = index match {
      case MetricIndex.Adhoc         => "Adhoc Metric Report"
      case MetricIndex.Periodic(idx) => s"Metric Report(index=$idx)"
    }
    override val isPivotal: Boolean = true
  }

  final case class MetricReset(
    index: MetricIndex,
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    snapshot: MetricSnapshot)
      extends MetricEvent {
    override val title: String = index match {
      case MetricIndex.Adhoc         => "Adhoc Metric Reset"
      case MetricIndex.Periodic(idx) => s"Metric Reset(index=$idx)"
    }
    override val isPivotal: Boolean = true
  }

  final case class InstantAlert(
    metricName: MetricName,
    timestamp: ZonedDateTime,
    serviceParams: ServiceParams,
    alertLevel: AlertLevel,
    message: String)
      extends ServiceEvent {
    override val title: String      = titles.instantAlert
    override val isPivotal: Boolean = true
  }

  sealed trait ActionEvent extends ServiceEvent {
    def actionInfo: ActionInfo // action runtime information
    def actionParams: ActionParams

    final def traceId: String                       = actionInfo.traceInfo.map(_.traceId).getOrElse("none")
    final override def serviceParams: ServiceParams = actionParams.serviceParams

    final def metricID: MetricID = actionParams.metricID
    final def actionId: String   = actionInfo.actionId
  }

  final case class ActionStart(actionParams: ActionParams, actionInfo: ActionInfo) extends ActionEvent {
    override def timestamp: ZonedDateTime = serviceParams.toZonedDateTime(actionInfo.launchTime)
    override val title: String            = titles.actionStart
    override val isPivotal: Boolean       = false
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
    override val title: String            = titles.actionRetry
    override val isPivotal: Boolean       = true
    val tookSoFar: Duration               = actionInfo.took(landTime)
  }

  sealed trait ActionResultEvent extends ActionEvent {
    def landTime: FiniteDuration
    final override def timestamp: ZonedDateTime = serviceParams.toZonedDateTime(landTime)
    final def took: Duration                    = actionInfo.took(landTime)
    def isDone: Boolean
    def output: Json
  }

  @Lenses
  final case class ActionFail(
    actionParams: ActionParams,
    actionInfo: ActionInfo,
    landTime: FiniteDuration,
    error: NJError,
    output: Json)
      extends ActionResultEvent {
    override val title: String      = titles.actionFail
    override val isDone: Boolean    = false
    override val isPivotal: Boolean = true
  }

  @Lenses
  final case class ActionComplete(
    actionParams: ActionParams,
    actionInfo: ActionInfo,
    landTime: FiniteDuration,
    output: Json // output of the action
  ) extends ActionResultEvent {
    override val title: String      = titles.actionComplete
    override val isDone: Boolean    = true
    override val isPivotal: Boolean = false
  }
}

private object titles {
  @inline final val serviceStart: String   = "(Re)Start Service"
  @inline final val serviceStop: String    = "Service Stopped"
  @inline final val servicePanic: String   = "Service Panic"
  @inline final val actionStart: String    = "Start Action"
  @inline final val actionRetry: String    = "Action Retrying"
  @inline final val actionFail: String     = "Action Failed"
  @inline final val actionComplete: String = "Action Completed"
  @inline final val instantAlert: String   = "Alert"
}
