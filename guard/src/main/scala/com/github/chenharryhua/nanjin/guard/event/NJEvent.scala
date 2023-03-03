package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.common.guard.ServiceName
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, AlertLevel, Digested, ServiceParams}
import io.circe.Json
import io.circe.generic.JsonCodec
import monocle.macros.Lenses
import org.typelevel.cats.time.instances.zoneddatetime

import java.time.{Duration, ZonedDateTime}
import java.util.UUID

@JsonCodec
sealed trait NJEvent extends Product with Serializable {
  def timestamp: ZonedDateTime // event timestamp - when the event occurs
  def serviceParams: ServiceParams
  def title: String

  final def serviceId: UUID          = serviceParams.serviceId
  final def serviceName: ServiceName = serviceParams.serviceName
  final def upTime: Duration         = serviceParams.upTime(timestamp)

}

object NJEvent extends zoneddatetime {
  implicit final val showNJEvent: Show[NJEvent] = cats.derived.semiauto.show[NJEvent]

  sealed trait ServiceEvent extends NJEvent

  final case class ServiceStart(serviceParams: ServiceParams, timestamp: ZonedDateTime) extends ServiceEvent {
    override val title: String = titles.serviceStart
  }

  final case class ServicePanic(
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    restartTime: ZonedDateTime,
    error: NJError)
      extends ServiceEvent {
    override val title: String = titles.servicePanic
  }

  final case class ServiceStop(
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    cause: ServiceStopCause)
      extends ServiceEvent {
    override val title: String = titles.serviceStop
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
  }

  sealed trait ActionEvent extends ServiceEvent {
    def actionInfo: ActionInfo // action runtime information
    final def traceId: String = actionInfo.traceInfo.map(_.traceId).getOrElse("none")

    final override def serviceParams: ServiceParams = actionInfo.actionParams.serviceParams

    final def digested: Digested         = actionInfo.actionParams.digested
    final def actionParams: ActionParams = actionInfo.actionParams
    final def actionId: String           = actionInfo.actionId

  }

  @Lenses
  final case class ActionStart(actionInfo: ActionInfo) extends ActionEvent {
    override val timestamp: ZonedDateTime = actionInfo.launchTime
    override val title: String            = titles.actionStart
  }

  final case class ActionRetry(
    actionInfo: ActionInfo,
    timestamp: ZonedDateTime,
    retriesSoFar: Int,
    resumeTime: ZonedDateTime,
    error: NJError)
      extends ActionEvent {
    override val title: String = titles.actionRetry

    val tookSoFar: Duration = Duration.between(actionInfo.launchTime, timestamp)
  }

  sealed trait ActionResultEvent extends ActionEvent {
    final def took: Duration = Duration.between(actionInfo.launchTime, timestamp)
    def isDone: Boolean
    def output: Json
  }

  @Lenses
  final case class ActionFail(actionInfo: ActionInfo, timestamp: ZonedDateTime, error: NJError, output: Json)
      extends ActionResultEvent {
    override val title: String   = titles.actionFail
    override val isDone: Boolean = false
  }

  @Lenses
  final case class ActionComplete(
    actionInfo: ActionInfo,
    timestamp: ZonedDateTime,
    output: Json // output of the action
  ) extends ActionResultEvent {
    override val title: String   = titles.actionComplete
    override val isDone: Boolean = true
  }

  sealed trait InstantEvent extends ServiceEvent {
    def digested: Digested
  }

  final case class InstantAlert(
    digested: Digested,
    timestamp: ZonedDateTime,
    serviceParams: ServiceParams,
    alertLevel: AlertLevel,
    message: String)
      extends InstantEvent {
    override val title: String = titles.instantAlert
  }

  @Lenses
  final case class PassThrough(
    digested: Digested,
    timestamp: ZonedDateTime,
    serviceParams: ServiceParams,
    value: Json)
      extends InstantEvent {
    override val title: String = titles.passThrough
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
  @inline final val passThrough: String    = "Pass Through"
}
