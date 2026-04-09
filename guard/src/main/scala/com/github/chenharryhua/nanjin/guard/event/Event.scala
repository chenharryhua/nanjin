package com.github.chenharryhua.nanjin.guard.event

import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams, UpTime}
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index.{Adhoc, Periodic}
import io.circe.Codec
import monocle.macros.{GenLens, GenPrism}
import monocle.{Optional, Prism}

sealed trait Event extends Product derives Codec.AsObject {
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

  final case class MetricsSnapshot(index: Index, serviceParams: ServiceParams, snapshot: Snapshot, took: Took)
      extends Event {
    override val timestamp: Timestamp = Timestamp(index.scrapeTime)
    val label: Label = index match
      case Index.Adhoc(_)       => Label("Report-Adhoc")
      case Index.Periodic(tick) => Label(s"Report-${tick.index}")
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

  /*
   * Optics
   */

  val metricsSnapshot: Prism[Event, MetricsSnapshot] = GenPrism[Event, Event.MetricsSnapshot]
  val reportedEvent: Prism[Event, ReportedEvent] = GenPrism[Event, Event.ReportedEvent]
  val serviceStart: Prism[Event, ServiceStart] = GenPrism[Event, Event.ServiceStart]
  val serviceStop: Prism[Event, ServiceStop] = GenPrism[Event, Event.ServiceStop]
  val servicePanic: Prism[Event, ServicePanic] = GenPrism[Event, Event.ServicePanic]

  val adhocSnapshot: Optional[Event, Adhoc] =
    metricsSnapshot
      .andThen(GenLens[MetricsSnapshot](_.index))
      .andThen(GenPrism[Index, Adhoc])

  val reportTick: Optional[Event, Tick] =
    metricsSnapshot
      .andThen(GenLens[MetricsSnapshot](_.index))
      .andThen(GenPrism[Index, Periodic])
      .andThen(GenLens[Periodic](_.tick))
}
