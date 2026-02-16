package com.github.chenharryhua.nanjin.guard.event

import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, Domain, ServiceParams}
import io.circe.Json
import io.circe.generic.JsonCodec

import java.time.{Duration, ZonedDateTime}

@JsonCodec
sealed trait Event extends Product {
  def timestamp: ZonedDateTime // event timestamp - when the event occurs
  def serviceParams: ServiceParams

  final def upTime: Duration = serviceParams.upTime(timestamp)
  def name: EventName
}

object Event {

  final case class ServiceStart(serviceParams: ServiceParams, tick: Tick) extends Event {
    override val timestamp: ZonedDateTime = tick.zoned(_.conclude)
    override val name: EventName = EventName.ServiceStart
  }

  final case class ServicePanic(serviceParams: ServiceParams, tick: Tick, error: Error) extends Event {
    override val timestamp: ZonedDateTime = tick.zoned(_.acquires)
    override val name: EventName = EventName.ServicePanic
  }

  final case class ServiceStop(
    serviceParams: ServiceParams,
    timestamp: ZonedDateTime,
    cause: ServiceStopCause)
      extends Event {
    override val name: EventName = EventName.ServiceStop
  }

  final case class ServiceMessage(
    serviceParams: ServiceParams,
    domain: Domain,
    timestamp: ZonedDateTime,
    correlation: Correlation,
    level: AlarmLevel,
    error: Option[Error],
    message: Json
  ) extends Event {
    override val name: EventName = EventName.ServiceMessage
  }

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
    override val name: EventName = EventName.MetricReport
  }

  final case class MetricReset(
    index: MetricIndex,
    serviceParams: ServiceParams,
    snapshot: MetricSnapshot,
    took: Duration)
      extends MetricEvent {
    override val timestamp: ZonedDateTime = index.launchTime
    override val name: EventName = EventName.MetricReset
  }
}
