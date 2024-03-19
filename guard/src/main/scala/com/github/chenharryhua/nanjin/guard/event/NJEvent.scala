package com.github.chenharryhua.nanjin.guard.event

import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, AlertLevel, MetricName, ServiceParams}
import io.circe.Json
import io.circe.generic.JsonCodec

import java.time.{Duration, Instant, ZonedDateTime}

@JsonCodec
sealed trait NJEvent extends Product with Serializable {
  def timestamp: ZonedDateTime // event timestamp - when the event occurs
  def serviceParams: ServiceParams

  final def upTime: Duration = serviceParams.upTime(timestamp)
}

object NJEvent {

  final case class ServiceStart(serviceParams: ServiceParams, tick: Tick) extends NJEvent {
    val timestamp: ZonedDateTime = tick.zonedWakeup
  }

  final case class ServicePanic(serviceParams: ServiceParams, error: NJError, tick: Tick) extends NJEvent {
    val timestamp: ZonedDateTime   = tick.acquire.atZone(tick.zoneId)
    val restartTime: ZonedDateTime = tick.zonedWakeup
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
    def landTime: Instant

    final override def timestamp: ZonedDateTime     = serviceParams.toZonedDateTime(landTime)
    final override def serviceParams: ServiceParams = actionParams.serviceParams
  }

  final case class ActionStart(
    actionParams: ActionParams,
    actionId: Int,
    launchTime: Instant,
    notes: Option[Json])
      extends ActionEvent {
    override val landTime: Instant = launchTime
  }

  final case class ActionRetry(
    actionParams: ActionParams,
    actionId: Int,
    launchTime: Option[Instant],
    error: NJError,
    tick: Tick)
      extends ActionEvent {
    override val landTime: Instant = tick.acquire
  }

  sealed trait ActionResultEvent extends ActionEvent {
    def notes: Option[Json]
    def launchTime: Option[Instant]
    final def took: Option[Duration] = launchTime.map(Duration.between(_, landTime))
  }

  final case class ActionFail(
    actionParams: ActionParams,
    actionId: Int,
    launchTime: Option[Instant],
    landTime: Instant,
    error: NJError,
    notes: Option[Json])
      extends ActionResultEvent

  final case class ActionDone(
    actionParams: ActionParams,
    actionId: Int,
    launchTime: Option[Instant],
    landTime: Instant,
    notes: Option[Json])
      extends ActionResultEvent
}
