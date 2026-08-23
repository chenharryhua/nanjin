package com.github.chenharryhua.nanjin.guard.event

import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.config.{
  Brief,
  Domain,
  ServiceIdentity,
  ServiceParams,
  StackTrace,
  Timestamp,
  UpTime
}
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index.{Adhoc, Periodic}
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.Snapshot
import io.circe.Codec
import monocle.macros.{GenLens, GenPrism}
import monocle.{Optional, Prism}

sealed trait Event extends Product derives Codec.AsObject {
  def timestamp: Timestamp // event timestamp - when the event occurs
  def serviceIdentity: ServiceIdentity

  final def upTime: UpTime = serviceIdentity.launchTime.upTime(timestamp)
}

object Event {

  final case class ServiceStart(serviceIdentity: ServiceIdentity, policy: Policy, brief: Brief, tick: Tick)
      extends Event {
    override val timestamp: Timestamp = Timestamp(tick.zoned(_.conclude))
  }

  final case class ServicePanic(
    serviceIdentity: ServiceIdentity,
    policy: Policy,
    brief: Brief,
    tick: Tick,

    stackTrace: StackTrace)
      extends Event {
    override val timestamp: Timestamp = Timestamp(tick.zoned(_.acquires))
  }

  final case class ServiceStop(
    serviceIdentity: ServiceIdentity,
    policy: Policy,
    brief: Brief,
    timestamp: Timestamp,
    cause: StopReason)
      extends Event

  final case class MetricsSnapshot(
    serviceIdentity: ServiceIdentity,
    index: Index,
    serviceParams: ServiceParams,
    snapshot: Snapshot,
    took: Took)
      extends Event {
    override val timestamp: Timestamp = Timestamp(index.scrapeTime)
  }

  final case class ReportedEvent(
    serviceIdentity: ServiceIdentity,
    serviceParams: ServiceParams,
    domain: Domain,
    timestamp: Timestamp,
    correlation: Correlation,
    level: LogLevel,
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
