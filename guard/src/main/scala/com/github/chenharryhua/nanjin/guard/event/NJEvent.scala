package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.datetime.DateTimeInstances
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, AlertLevel, MetricName, ServiceParams}
import io.circe.Json
import io.circe.generic.JsonCodec

import java.time.{Duration, ZonedDateTime}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

@JsonCodec
sealed trait NJEvent extends Product with Serializable {
  def timestamp: ZonedDateTime // event timestamp - when the event occurs
  def serviceParams: ServiceParams

  final def upTime: Duration = serviceParams.upTime(timestamp)
}

object NJEvent extends DateTimeInstances {
  implicit final val showNJEvent: Show[NJEvent] = cats.derived.semiauto.show[NJEvent]

  final case class ServiceStart(serviceParams: ServiceParams, tick: Tick) extends NJEvent {
    val timestamp: ZonedDateTime = tick.wakeup.atZone(tick.zoneId)
  }

  final case class ServicePanic(serviceParams: ServiceParams, error: NJError, tick: Tick) extends NJEvent {
    val timestamp: ZonedDateTime   = tick.acquire.atZone(tick.zoneId)
    val restartTime: ZonedDateTime = tick.wakeup.atZone(tick.zoneId)
  }

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
    def actionParams: ActionParams
    def actionId: Int
    def launchTime: FiniteDuration
    def landTime: FiniteDuration

    final override def timestamp: ZonedDateTime     = serviceParams.toZonedDateTime(landTime)
    final override def serviceParams: ServiceParams = actionParams.serviceParams
  }

  final case class ActionStart(
    actionParams: ActionParams,
    actionId: Int,
    launchTime: FiniteDuration,
    notes: Option[Json])
      extends ActionEvent {
    override def landTime: FiniteDuration = launchTime
  }

  final case class ActionRetry(
    actionParams: ActionParams,
    actionId: Int,
    launchTime: FiniteDuration,
    error: NJError,
    tick: Tick)
      extends ActionEvent {
    override def landTime: FiniteDuration = FiniteDuration(tick.acquire.toEpochMilli, TimeUnit.MILLISECONDS)
  }

  sealed trait ActionResultEvent extends ActionEvent {
    def notes: Option[Json]
    final def took: Duration = (landTime - launchTime).toJava
  }

  final case class ActionFail(
    actionParams: ActionParams,
    actionId: Int,
    launchTime: FiniteDuration,
    landTime: FiniteDuration,
    error: NJError,
    notes: Option[Json])
      extends ActionResultEvent

  final case class ActionDone(
    actionParams: ActionParams,
    actionId: Int,
    launchTime: FiniteDuration,
    landTime: FiniteDuration,
    notes: Option[Json])
      extends ActionResultEvent

  // filters

  final def isPivotalEvent(evt: NJEvent): Boolean = evt match {
    case _: ActionDone  => false
    case _: ActionStart => false
    case _              => true
  }

  final def isServiceEvent(evt: NJEvent): Boolean = evt match {
    case _: ActionEvent => false
    case _              => true
  }

  final def isActionDone(evt: ActionResultEvent): Boolean = evt match {
    case _: ActionFail => false
    case _: ActionDone => true
  }
}
