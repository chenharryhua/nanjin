package com.github.chenharryhua.nanjin.guard.event

import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, Domain, ServiceParams, UpTime}
import com.github.chenharryhua.nanjin.guard.event.MetricsReportData.Index
import io.circe.Json
import io.circe.generic.JsonCodec

@JsonCodec
sealed trait Event extends Product {
  def timestamp: Timestamp // event timestamp - when the event occurs
  def serviceParams: ServiceParams

  final def upTime: UpTime = serviceParams.upTime(timestamp.value)
  def name: EventName
}

object Event {

  final case class ServiceStart(serviceParams: ServiceParams, tick: Tick) extends Event {
    override val timestamp: Timestamp = Timestamp(tick.zoned(_.conclude))
    override val name: EventName = EventName.ServiceStart
  }

  final case class ServicePanic(serviceParams: ServiceParams, tick: Tick, error: Error) extends Event {
    override val timestamp: Timestamp = Timestamp(tick.zoned(_.acquires))
    override val name: EventName = EventName.ServicePanic
  }

  final case class ServiceStop(serviceParams: ServiceParams, timestamp: Timestamp, cause: ServiceStopCause)
      extends Event {
    override val name: EventName = EventName.ServiceStop
  }

  final case class ServiceMessage(
    serviceParams: ServiceParams,
    domain: Domain,
    timestamp: Timestamp,
    correlation: Correlation,
    level: AlarmLevel,
    error: Option[Error],
    message: Json
  ) extends Event {
    override val name: EventName = EventName.ServiceMessage
  }

  sealed trait MetricsEvent extends Event {
    def index: Index
    def snapshot: MetricSnapshot
    def took: Took // time took to retrieve snapshot
  }

  final case class MetricsReport(
    index: Index,
    serviceParams: ServiceParams,
    snapshot: MetricSnapshot,
    took: Took)
      extends MetricsEvent {
    override val timestamp: Timestamp = Timestamp(index.launchTime)
    override val name: EventName = EventName.MetricsReport
  }

  final case class MetricsReset(
    index: Index,
    serviceParams: ServiceParams,
    snapshot: MetricSnapshot,
    took: Took)
      extends MetricsEvent {
    override val timestamp: Timestamp = Timestamp(index.launchTime)
    override val name: EventName = EventName.MetricsReset
  }
}
