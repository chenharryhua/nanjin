package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.derived.auto.show.*
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.guard.ServiceName
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Digested, Importance, ServiceParams}
import io.circe.{Encoder, Json}
import io.circe.generic.JsonCodec

import java.time.{Duration, ZonedDateTime}
import java.util.UUID

@JsonCodec
sealed trait NJEvent {
  def timestamp: ZonedDateTime // event timestamp - when the event occurs
  def serviceParams: ServiceParams
  def title: String

  final def serviceID: UUID          = serviceParams.serviceID
  final def serviceName: ServiceName = serviceParams.serviceName
  final def upTime: Duration         = serviceParams.upTime(timestamp)

  final def asJson: Json = Encoder[NJEvent].apply(this)
}

object NJEvent {
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
    def snapshot: MetricSnapshot
  }

  final case class MetricReport(
    reportType: MetricReportType,
    serviceParams: ServiceParams,
    ongoings: List[ActionInfo],
    timestamp: ZonedDateTime,
    snapshot: MetricSnapshot,
    restartTime: Option[ZonedDateTime])
      extends MetricEvent {
    val isPanic: Boolean       = restartTime.nonEmpty
    val isUp: Boolean          = restartTime.isEmpty
    override val title: String = reportType.show
  }

  final case class MetricReset(
    resetType: MetricResetType,
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    snapshot: MetricSnapshot)
      extends MetricEvent {
    override val title: String = resetType.show
  }

  sealed trait ActionEvent extends ServiceEvent {
    def actionInfo: ActionInfo // action runtime information

    final override def serviceParams: ServiceParams = actionInfo.actionParams.serviceParams

    final def name: Digested             = actionInfo.actionParams.name
    final def actionParams: ActionParams = actionInfo.actionParams
    final def actionID: Int              = actionInfo.actionID

    final def took: Duration = Duration.between(actionInfo.launchTime, timestamp)
  }

  final case class ActionStart(actionInfo: ActionInfo, input: Json) extends ActionEvent {
    override val timestamp: ZonedDateTime = actionInfo.launchTime
    override val title: String            = titles.actionStart
  }

  final case class ActionRetry(
    actionInfo: ActionInfo,
    timestamp: ZonedDateTime,
    retriesSoFar: Int,
    nextRetryTime: ZonedDateTime,
    error: NJError)
      extends ActionEvent {
    override val title: String = titles.actionRetry
  }

  sealed trait ActionResultEvent extends ActionEvent {
    def numRetries: Int
  }

  final case class ActionFail(
    actionInfo: ActionInfo,
    timestamp: ZonedDateTime,
    numRetries: Int, // number of retries before giving up
    input: Json, // input of the action
    error: NJError)
      extends ActionResultEvent {
    override val title: String = titles.actionFail
  }

  final case class ActionSucc(
    actionInfo: ActionInfo,
    timestamp: ZonedDateTime,
    numRetries: Int, // number of retries before success
    output: Json // output of the action
  ) extends ActionResultEvent {
    override val title: String = titles.actionSucc
  }

  sealed trait InstantEvent extends ServiceEvent {
    def name: Digested
  }

  final case class InstantAlert(
    name: Digested,
    timestamp: ZonedDateTime,
    serviceParams: ServiceParams,
    importance: Importance,
    message: String)
      extends InstantEvent {
    override val title: String = titles.instantAlert
  }

  final case class PassThrough(
    name: Digested,
    timestamp: ZonedDateTime,
    serviceParams: ServiceParams,
    isError: Boolean, // the payload json represent an error
    value: Json)
      extends InstantEvent {
    override val title: String = titles.passThrough
  }

}

private object titles {
  @inline final val serviceStart: String = "(Re)Start Service"
  @inline final val serviceStop: String  = "Service Stopped"
  @inline final val servicePanic: String = "Service Panic"
  @inline final val actionStart: String  = "Start Action"
  @inline final val actionRetry: String  = "Action Retrying"
  @inline final val actionFail: String   = "Action Failed"
  @inline final val actionSucc: String   = "Action Succed"
  @inline final val instantAlert: String = "Alert"
  @inline final val passThrough: String  = "Pass Through"
}
