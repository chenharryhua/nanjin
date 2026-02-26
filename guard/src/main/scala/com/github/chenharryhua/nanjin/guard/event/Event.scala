package com.github.chenharryhua.nanjin.guard.event

import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams, UpTime}
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.{Index, Kind}
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

  final case class ServiceStop(serviceParams: ServiceParams, timestamp: Timestamp, cause: StopReason)
      extends Event

  final case class MetricsSnapshot(
    index: Index,
    serviceParams: ServiceParams,
    snapshot: Snapshot,
    kind: Kind,
    took: Took)
      extends Event {
    override val timestamp: Timestamp = Timestamp(index.launchTime)
    val label: Label = Label {
      index match {
        case ac @ Index.Adhoc(_)  => s"${kind.show}-${ac.productPrefix}"
        case Index.Periodic(tick) => s"${kind.show}-${tick.index}"
      }
    }
  }

  final case class ReportedEvent(
    serviceParams: ServiceParams,
    domain: Domain,
    timestamp: Timestamp,
    correlation: Correlation,
    level: AlarmLevel,
    stackTrace: Option[StackTrace],
    message: Message
  ) extends Event
}
