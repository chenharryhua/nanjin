package com.github.chenharryhua.nanjin.guard.event

import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.config.{
  Brief,
  Domain,
  ServiceIdentity,
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

/** The service event model.
  *
  * All events share a stable [[ServiceIdentity]] that identifies the running service instance, and a
  * [[Timestamp]] indicating when the event occurred. The `upTime` is derived from the difference between
  * launch time and event timestamp.
  *
  * ===Event Types===
  *
  *   - '''ServiceStart''' — emitted when the service starts (or restarts after a panic). Carries the restart
  *     `policy`, `brief` metadata, and the `tick` that triggered the start.
  *
  *   - '''ServicePanic''' — emitted when the service crashes but will be retried. Carries the restart
  *     `policy`, `brief`, the `tick` (with snooze duration until next retry), and the `stackTrace` of the
  *     failure.
  *
  *   - '''ServiceStop''' — emitted when the service terminates (successfully, by exception after exhausting
  *     retries, or by cancellation). Carries the restart `policy`, `brief`, and the `cause` of termination.
  *
  *   - '''MetricsSnapshot''' — periodic or ad-hoc scrape of all registered metrics. Carries the report
  *     `policy`, an `index` (periodic tick or ad-hoc timestamp), the full `snapshot` of metric values, and
  *     how long the scrape `took`.
  *
  *   - '''ReportedEvent''' — a user-emitted log message via `heraldLogger` or `logger`. Carries the
  *     `domain`, a unique `correlation` id, the `level`, optional `stackTrace`, and the JSON `message`.
  */
sealed trait Event extends Product derives Codec.AsObject {
  def timestamp: Timestamp // event timestamp - when the event occurs
  def serviceIdentity: ServiceIdentity

  final def upTime: UpTime = serviceIdentity.launchTime.upTime(timestamp)
}

object Event {

  /** Emitted when the service starts or restarts after a panic.
    *
    * @param serviceIdentity
    *   stable identity of the running service instance
    * @param policy
    *   the restart policy governing retry behavior
    * @param brief
    *   user-provided metadata attached at service configuration time
    * @param tick
    *   the tick that triggered this start (index 0 for initial start, >0 for restarts)
    */
  final case class ServiceStart(serviceIdentity: ServiceIdentity, policy: Policy, brief: Brief, tick: Tick)
      extends Event {
    override val timestamp: Timestamp = Timestamp(tick.zoned(_.conclude))
  }

  /** Emitted when the service crashes but the restart policy allows a retry.
    *
    * @param serviceIdentity
    *   stable identity of the running service instance
    * @param policy
    *   the restart policy governing retry behavior
    * @param brief
    *   user-provided metadata
    * @param tick
    *   the tick representing the retry schedule (snooze = time until next attempt)
    * @param stackTrace
    *   root-cause stack trace of the failure
    */
  final case class ServicePanic(
    serviceIdentity: ServiceIdentity,
    policy: Policy,
    brief: Brief,
    tick: Tick,
    stackTrace: StackTrace)
      extends Event {
    override val timestamp: Timestamp = Timestamp(tick.zoned(_.acquires))
  }

  /** Emitted when the service terminates.
    *
    * @param serviceIdentity
    *   stable identity of the running service instance
    * @param policy
    *   the restart policy that was in effect
    * @param brief
    *   user-provided metadata
    * @param timestamp
    *   when the stop occurred
    * @param cause
    *   reason for termination (Successfully, ByException, ByCancellation, or Maintenance)
    */
  final case class ServiceStop(
    serviceIdentity: ServiceIdentity,
    policy: Policy,
    brief: Brief,
    timestamp: Timestamp,
    cause: StopReason)
      extends Event

  /** Periodic or ad-hoc scrape of all registered metrics.
    *
    * @param serviceIdentity
    *   stable identity of the running service instance
    * @param policy
    *   the metrics reporting policy that scheduled this snapshot
    * @param index
    *   either Periodic (with a tick) or Adhoc (with a timestamp)
    * @param snapshot
    *   full snapshot of counters, meters, timers, histograms, and gauges
    * @param took
    *   wall-clock duration of the scrape operation
    */
  final case class MetricsSnapshot(
    serviceIdentity: ServiceIdentity,
    policy: Policy,
    index: Index,
    snapshot: Snapshot,
    took: Took)
      extends Event {
    override val timestamp: Timestamp = index.scrapeTime
  }

  /** A user-emitted log message published through the service's logging facilities.
    *
    * @param serviceIdentity
    *   stable identity of the running service instance
    * @param domain
    *   the domain under which this message was logged (default or via `withDomain`)
    * @param timestamp
    *   when the message was created
    * @param correlation
    *   unique correlation id for tracing this log entry
    * @param level
    *   log severity (Debug, Info, Good, Warn, Error)
    * @param stackTrace
    *   optional stack trace if an exception was attached
    * @param message
    *   the JSON-encoded message payload
    */
  final case class ReportedEvent(
    serviceIdentity: ServiceIdentity,
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
