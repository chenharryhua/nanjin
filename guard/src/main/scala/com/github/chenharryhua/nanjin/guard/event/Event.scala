package com.github.chenharryhua.nanjin.guard.event

import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import io.circe.Json
import io.circe.generic.JsonCodec

import java.time.{Duration, ZonedDateTime}

@JsonCodec
sealed trait Event extends Product with Serializable {
  def timestamp: ZonedDateTime // event timestamp - when the event occurs
  def serviceParams: ServiceParams

  final def upTime: Duration = serviceParams.upTime(timestamp)
}

object Event {

  final case class ServiceStart(serviceParams: ServiceParams, tick: Tick) extends Event {
    val timestamp: ZonedDateTime = tick.zonedWakeup
  }

  final case class ServicePanic(serviceParams: ServiceParams, tick: Tick, error: Error) extends Event {
    val timestamp: ZonedDateTime = tick.zonedAcquire
  }

  final case class ServiceStop(
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    cause: ServiceStopCause)
      extends Event

  final case class ServiceMessage(
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    token: Int,
    level: AlarmLevel,
    error: Option[Error],
    message: Json
  ) extends Event

  sealed trait MetricEvent extends Event {
    def index: MetricIndex
    def snapshot: MetricSnapshot
    def took: Duration // time took to retrieve snapshot
  }

  final case class MetricReport(
    index: MetricIndex,
    serviceParams: ServiceParams,
    snapshot: MetricSnapshot,
    took: Duration)
      extends MetricEvent {
    override val timestamp: ZonedDateTime = index.launchTime
  }

  final case class MetricReset(
    index: MetricIndex,
    serviceParams: ServiceParams,
    snapshot: MetricSnapshot,
    took: Duration)
      extends MetricEvent {
    override val timestamp: ZonedDateTime = index.launchTime
  }
}
