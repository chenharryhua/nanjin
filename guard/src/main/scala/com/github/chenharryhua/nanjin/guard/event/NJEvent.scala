package com.github.chenharryhua.nanjin.guard.event

import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, MetricName, ServiceParams}
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

  final case class ServicePanic(serviceParams: ServiceParams, tick: Tick, error: NJError) extends NJEvent {
    val timestamp: ZonedDateTime = tick.zonedAcquire
  }

  final case class ServiceStop(
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    cause: ServiceStopCause)
      extends NJEvent

  final case class ServiceMessage(
    metricName: MetricName,
    timestamp: ZonedDateTime,
    serviceParams: ServiceParams,
    level: AlarmLevel,
    message: Json)
      extends NJEvent

  sealed trait MetricEvent extends NJEvent {
    def index: MetricIndex
    def snapshot: MetricSnapshot
    final def took: Duration = Duration.between(index.launchTime, timestamp)
  }

  final case class MetricReport(
    index: MetricIndex,
    serviceParams: ServiceParams,
    snapshot: MetricSnapshot,
    timestamp: ZonedDateTime) // land time
      extends MetricEvent

  final case class MetricReset(
    index: MetricIndex,
    serviceParams: ServiceParams,
    snapshot: MetricSnapshot,
    timestamp: ZonedDateTime) // land time
      extends MetricEvent
}
