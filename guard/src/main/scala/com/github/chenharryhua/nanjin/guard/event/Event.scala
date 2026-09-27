package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.common.logging.{LogLevel, LogLink, MDC}
import com.github.chenharryhua.nanjin.guard.config.{
  Brief,
  Domain,
  ServiceIdentity,
  StackTrace,
  Timestamp,
  UpTime
}
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.Snapshot
import io.circe.Codec
import monocle.macros.{GenLens, GenPrism}
import monocle.{Optional, Prism}

/** The service event model.
  *
  * All events share a stable `ServiceIdentity` that identifies the running service instance, and a
  * `Timestamp` indicating when the event occurred. The `upTime` is derived from the difference between launch
  * time and event timestamp.
  */
sealed trait Event extends Product derives Codec.AsObject {
  def serviceIdentity: ServiceIdentity
  def logLink: Option[LogLink]
  def timestamp: Timestamp // event timestamp - when the event occurs

  final def upTime: UpTime = serviceIdentity.launchTime.upTime(timestamp)
}

object Event {

  /** Emitted when the service starts or restarts after a panic.
    *
    * @param serviceIdentity
    *   stable identity of the running service instance
    * @param brief
    *   user-provided metadata attached at service configuration time
    * @param tick
    *   the tick that triggered this start (index 0 for initial start, >0 for restarts)
    */
  final case class ServiceStart(
    serviceIdentity: ServiceIdentity,
    logLink: Option[LogLink],
    brief: Brief,
    tick: Tick)
      extends Event {
    override val timestamp: Timestamp = Timestamp(tick.zoned(_.conclude))
  }

  /** Emitted when the service crashes but the restart policy allows a retry.
    *
    * @param serviceIdentity
    *   stable identity of the running service instance
    * @param brief
    *   user-provided metadata
    * @param tick
    *   the tick representing the retry schedule (snooze = time until next attempt)
    * @param stackTrace
    *   root-cause stack trace of the failure
    */
  final case class ServicePanic(
    serviceIdentity: ServiceIdentity,
    logLink: Option[LogLink],
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
    * @param brief
    *   user-provided metadata
    * @param timestamp
    *   when the stop occurred
    * @param cause
    *   reason for termination (Successfully, ByException, ByCancellation, or Maintenance)
    */
  final case class ServiceStop(
    serviceIdentity: ServiceIdentity,
    logLink: Option[LogLink],
    brief: Brief,
    timestamp: Timestamp,
    cause: StopReason)
      extends Event

  /** Periodic or ad-hoc scrape of all registered metrics.
    *
    * @param serviceIdentity
    *   stable identity of the running service instance
    * @param index
    *   either Periodic (with a tick) or Adhoc (with a timestamp)
    * @param snapshot
    *   full snapshot of counters, meters, timers, histograms, and gauges
    * @param took
    *   wall-clock duration of the scrape operation
    */
  final case class MetricsSnapshot(
    serviceIdentity: ServiceIdentity,
    logLink: Option[LogLink],
    index: MetricsSnapshot.Index,
    snapshot: Snapshot,
    took: Took)
      extends Event {
    override val timestamp: Timestamp = index.scrapeTime
  }
  object MetricsSnapshot:
    sealed trait Index derives Codec.AsObject:
      def scrapeTime: Timestamp
    end Index

    final case class Adhoc(scrapeTime: Timestamp) extends Index
    final case class Periodic(tick: Tick) extends Index:
      override val scrapeTime: Timestamp = Timestamp(tick.zoned(_.conclude))

    /** Renders an `Index` to a short label: `"Adhoc"` for ad-hoc scrapes, or the tick index for periodic
      * ones.
      *
      * This is a wire contract, not just a display concern: `PrettyJsonTranslator` serializes `index.show`
      * into the emitted JSON payload (under the `metrics_snapshot` key). Changing these strings changes the
      * observed output, so treat edits here as a wire-format change.
      */
    given Show[Index]:
      override def show(t: Index): String = t match {
        case Adhoc(_)       => "Adhoc"
        case Periodic(tick) => s"${tick.index}"
      }
  end MetricsSnapshot

  /** A user-emitted log message published through the service's logging facilities.
    *
    * @param serviceIdentity
    *   stable identity of the running service instance
    * @param logLink
    *   optional link to the external log record for this event
    * @param timestamp
    *   when the message was created
    * @param domain
    *   domain under which the message was logged
    * @param correlation
    *   unique identifier for tracing this log entry
    * @param logRecord
    *   message payload, severity, optional stack trace, and diagnostic context
    */
  final case class ReportedEvent(
    serviceIdentity: ServiceIdentity,
    logLink: Option[LogLink],
    timestamp: Timestamp,
    domain: Domain,
    correlation: Correlation,
    logRecord: ReportedEvent.LogRecord
  ) extends Event
  object ReportedEvent {

    /** The log payload and metadata carried by a reported event.
      *
      * @param message
      *   JSON-encoded log payload
      * @param level
      *   severity of the log record
      * @param stackTrace
      *   optional stack trace attached to the record
      * @param mdc
      *   mapped diagnostic context associated with the record
      */
    final case class LogRecord(message: Message, level: LogLevel, stackTrace: Option[StackTrace], mdc: MDC)
        derives Codec.AsObject
  }

  /*
   * Optics
   */

  val metricsSnapshot: Prism[Event, MetricsSnapshot] = GenPrism[Event, Event.MetricsSnapshot]
  val reportedEvent: Prism[Event, ReportedEvent] = GenPrism[Event, Event.ReportedEvent]
  val serviceStart: Prism[Event, ServiceStart] = GenPrism[Event, Event.ServiceStart]
  val serviceStop: Prism[Event, ServiceStop] = GenPrism[Event, Event.ServiceStop]
  val servicePanic: Prism[Event, ServicePanic] = GenPrism[Event, Event.ServicePanic]

  val adhocSnapshot: Optional[Event, MetricsSnapshot.Adhoc] =
    metricsSnapshot
      .andThen(GenLens[MetricsSnapshot](_.index))
      .andThen(GenPrism[MetricsSnapshot.Index, MetricsSnapshot.Adhoc])

  val reportTick: Optional[Event, Tick] =
    metricsSnapshot
      .andThen(GenLens[MetricsSnapshot](_.index))
      .andThen(GenPrism[MetricsSnapshot.Index, MetricsSnapshot.Periodic])
      .andThen(GenLens[MetricsSnapshot.Periodic](_.tick))

  val mdc: Optional[Event, MDC] =
    reportedEvent
      .andThen(GenLens[ReportedEvent](_.logRecord))
      .andThen(GenLens[ReportedEvent.LogRecord](_.mdc))
}
