package com.github.chenharryhua.nanjin.guard.event

import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, Domain, ServiceParams, UpTime}
import io.circe.generic.JsonCodec

@JsonCodec
sealed trait Event extends Product {
  def timestamp: Timestamp // event timestamp - when the event occurs
  def serviceParams: ServiceParams

  final def upTime: UpTime = serviceParams.upTime(timestamp.value)
}

object Event {

  final case class ServiceStart(serviceParams: ServiceParams, tick: Tick) extends Event {
    override val timestamp: Timestamp = Timestamp(tick.zoned(_.conclude))
  }

  final case class ServicePanic(serviceParams: ServiceParams, tick: Tick, stackTrace: StackTrace)
      extends Event {
    override val timestamp: Timestamp = Timestamp(tick.zoned(_.acquires))
  }

  final case class ServiceStop(serviceParams: ServiceParams, timestamp: Timestamp, cause: ServiceStopCause)
      extends Event

  final case class ServiceMessage(
    serviceParams: ServiceParams,
    domain: Domain,
    timestamp: Timestamp,
    correlation: Correlation,
    level: AlarmLevel,
    stackTrace: Option[StackTrace],
    message: Message
  ) extends Event

  sealed trait MetricsEvent extends Event {
    def index: Index
    def snapshot: Snapshot
    def took: Took // time took to retrieve snapshot
  }

  final case class MetricsReport(index: Index, serviceParams: ServiceParams, snapshot: Snapshot, took: Took)
      extends MetricsEvent {
    override val timestamp: Timestamp = Timestamp(index.launchTime)
  }

  final case class MetricsReset(index: Index, serviceParams: ServiceParams, snapshot: Snapshot, took: Took)
      extends MetricsEvent {
    override val timestamp: Timestamp = Timestamp(index.launchTime)
  }
}
