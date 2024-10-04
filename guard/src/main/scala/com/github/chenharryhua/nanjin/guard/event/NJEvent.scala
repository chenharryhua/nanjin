package com.github.chenharryhua.nanjin.guard.event

import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, AlertLevel, MetricName, ServiceParams}
import io.circe.Json
import io.circe.generic.JsonCodec

import java.time.{Duration, ZonedDateTime}

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
    val timestamp: ZonedDateTime = tick.zonedAcquire
  }

  final case class ServiceStop(
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    cause: ServiceStopCause)
      extends NJEvent

  final case class ServiceAlert(
    alertID: UniqueToken,
    metricName: MetricName,
    timestamp: ZonedDateTime,
    serviceParams: ServiceParams,
    alertLevel: AlertLevel,
    message: Json)
      extends NJEvent

  sealed trait MetricEvent extends NJEvent {
    def index: MetricIndex
    def snapshot: MetricSnapshot
    final def took: Duration = Duration.between(index.launchTime, timestamp)
  }

  final case class MetricReport(
    index: MetricIndex, // launch time
    serviceParams: ServiceParams,
    snapshot: MetricSnapshot,
    timestamp: ZonedDateTime) // land time
      extends MetricEvent

  final case class MetricReset(
    index: MetricIndex, // launch time
    serviceParams: ServiceParams,
    snapshot: MetricSnapshot,
    timestamp: ZonedDateTime) // land time
      extends MetricEvent

  sealed trait ActionEvent extends NJEvent {
    def actionID: UniqueToken
    def actionParams: ActionParams
    final override def serviceParams: ServiceParams = actionParams.serviceParams
  }

  final case class ActionStart(
    actionID: UniqueToken,
    actionParams: ActionParams,
    timestamp: ZonedDateTime,
    notes: Json)
      extends ActionEvent

  final case class ActionRetry(
    actionID: UniqueToken,
    actionParams: ActionParams,
    error: NJError,
    notes: Json,
    tick: Tick)
      extends ActionEvent {
    override val timestamp: ZonedDateTime = tick.zonedAcquire
  }

  sealed trait ActionResultEvent extends ActionEvent {
    def notes: Json
    def isDone: Boolean
  }

  final case class ActionFail(
    actionID: UniqueToken,
    actionParams: ActionParams,
    timestamp: ZonedDateTime, // land time
    error: NJError,
    notes: Json)
      extends ActionResultEvent {
    override val isDone: Boolean = false
  }

  final case class ActionDone(
    actionID: UniqueToken,
    actionParams: ActionParams,
    launchTime: ZonedDateTime,
    timestamp: ZonedDateTime, // land time
    notes: Json)
      extends ActionResultEvent {
    override val isDone: Boolean = true

    lazy val took: Duration = Duration.between(launchTime, timestamp)
  }
}
