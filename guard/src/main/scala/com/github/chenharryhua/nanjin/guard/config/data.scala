package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import cats.kernel.Eq
import cats.syntax.order.given
import cats.syntax.show.given
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.common.OpaqueLift
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import io.circe.{Codec, Decoder, Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.typelevel.cats.time.instances.localdatetime.localdatetimeInstances
import org.typelevel.cats.time.instances.localtime.localtimeInstances
import org.typelevel.cats.time.zoneidInstances

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, ZoneId, ZonedDateTime}
import java.util.UUID
import scala.concurrent.duration.given
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.DurationConverters.given

/** Non-breaking space char used as indentation on platforms that collapse regular whitespace (e.g. Teams
  * Adaptive Cards).
  */
final val NBSP_CHAR: Char = '\u00A0'

// ---------------- StackTrace ----------------

/** Root-cause stack trace captured from a `Throwable`. Stored as a list of individual frame strings (with tab
  * indentation stripped) rather than a single blob, enabling translators to format and truncate per-line.
  * Extraction uses `ExceptionUtils.getRootCauseStackTraceList` so the deepest cause appears first.
  */
opaque type StackTrace = List[String]
object StackTrace:
  final private val NBSP_INDENT: String = String.valueOf(NBSP_CHAR) * 2
  def apply(ex: Throwable): StackTrace =
    ExceptionUtils.getRootCauseStackTraceList(ex).asScala.map(_.replace("\t", "")).toList
  extension (st: StackTrace)
    inline def value: List[String] = st
    def headOption: Option[String] = st.headOption
    def nbspIndented: String = st.mkString(s"\n$NBSP_INDENT")

  given Show[StackTrace] = _.mkString("\n  ")
  given Encoder[StackTrace] = OpaqueLift.lift[StackTrace, List[String], Encoder]
  given Decoder[StackTrace] = OpaqueLift.lift[StackTrace, List[String], Decoder]
end StackTrace

// ---------------- Task ----------------
opaque type Task = String
object Task:
  def apply(value: String): Task = value
  extension (t: Task) inline def value: String = t

  given Show[Task] = OpaqueLift.lift[Task, String, Show]
  given Encoder[Task] = OpaqueLift.lift[Task, String, Encoder]
  given Decoder[Task] = OpaqueLift.lift[Task, String, Decoder]
end Task

// ---------------- Service ----------------
opaque type Service = String
object Service:
  def apply(value: String): Service = value
  extension (s: Service) inline def value: String = s

  given Show[Service] = OpaqueLift.lift[Service, String, Show]
  given Encoder[Service] = OpaqueLift.lift[Service, String, Encoder]
  given Decoder[Service] = OpaqueLift.lift[Service, String, Decoder]
end Service

// ---------------- ServiceId ----------------

/** A unique identifier generated for each service instance at launch time. A new UUID is minted on every JVM
  * start; it does not change across panic-triggered restarts within the same process lifetime.
  */
opaque type ServiceId = UUID
object ServiceId:
  def apply(value: UUID): ServiceId = value
  extension (s: ServiceId) inline def value: UUID = s

  given Show[ServiceId] = OpaqueLift.lift[ServiceId, UUID, Show]
  given Encoder[ServiceId] = OpaqueLift.lift[ServiceId, UUID, Encoder]
  given Decoder[ServiceId] = OpaqueLift.lift[ServiceId, UUID, Decoder]
end ServiceId

// ---------------- Homepage ----------------
opaque type Homepage = String
object Homepage:
  def apply(value: String): Homepage = value
  extension (h: Homepage) inline def value: String = h

  given Encoder[Homepage] = OpaqueLift.lift[Homepage, String, Encoder]
  given Decoder[Homepage] = OpaqueLift.lift[Homepage, String, Decoder]
end Homepage

// ---------------- Port ----------------
opaque type Port = Int
object Port:
  def apply(value: Int): Port = value
  extension (p: Port) inline def value: Int = p

  given Show[Port] = OpaqueLift.lift[Port, Int, Show]
  given Encoder[Port] = OpaqueLift.lift[Port, Int, Encoder]
  given Decoder[Port] = OpaqueLift.lift[Port, Int, Decoder]
end Port

// ---------------- Brief ----------------

/** A service brief is a JSON document carrying user-provided metadata that travels with every service
  * lifecycle event (start, panic, stop). Typical content includes deployment context such as ECS task
  * definitions, CloudWatch log options, build info, or any custom annotations added via
  * `ServiceConfig.addBrief`. The name derives from the noun meaning: "a concise informational document."
  */
opaque type Brief = Json
object Brief:
  def apply(value: Json): Brief = value
  extension (b: Brief) inline def value: Json = b

  given Show[Brief] = _.value.spaces2
  given Encoder[Brief] = OpaqueLift.lift[Brief, Json, Encoder]
  given Decoder[Brief] = OpaqueLift.lift[Brief, Json, Decoder]
end Brief

// ---------------- TimeZone ----------------
opaque type TimeZone = ZoneId
object TimeZone:
  def apply(zoneId: ZoneId): TimeZone = zoneId
  extension (tz: TimeZone) inline def value: ZoneId = tz

  given Show[TimeZone] = OpaqueLift.lift[TimeZone, ZoneId, Show]
  given Encoder[TimeZone] = OpaqueLift.lift[TimeZone, ZoneId, Encoder]
  given Decoder[TimeZone] = OpaqueLift.lift[TimeZone, ZoneId, Decoder]
end TimeZone

// ---------------- UpTime ----------------
opaque type UpTime = Duration
object UpTime:
  def apply(duration: Duration): UpTime = duration
  extension (upTime: UpTime) inline def value: Duration = upTime

  given Show[UpTime] = defaultFormatter.format(_)
  given Encoder[UpTime] = OpaqueLift.lift[UpTime, Duration, Encoder]
  given Decoder[UpTime] = OpaqueLift.lift[UpTime, Duration, Decoder]
end UpTime

// ---------------- Capacity ----------------
opaque type Capacity = Int
object Capacity:
  def apply(value: Int): Capacity = value

  extension (c: Capacity) inline def value: Int = c

  given Show[Capacity] = OpaqueLift.lift[Capacity, Int, Show]
  given Encoder[Capacity] = OpaqueLift.lift[Capacity, Int, Encoder]
  given Decoder[Capacity] = OpaqueLift.lift[Capacity, Int, Decoder]
end Capacity

// ---------------- Domain ----------------
opaque type Domain = String
object Domain:
  def apply(value: String): Domain = value
  extension (d: Domain) inline def value: String = d

  given Show[Domain] = OpaqueLift.lift[Domain, String, Show]
  given Eq[Domain] = OpaqueLift.lift[Domain, String, Eq]
  given Encoder[Domain] = OpaqueLift.lift[Domain, String, Encoder]
  given Decoder[Domain] = OpaqueLift.lift[Domain, String, Decoder]
end Domain

// ---------------- Timestamp ----------------

/** The moment an event occurred, expressed as a `ZonedDateTime` in the service's configured time zone. This
  * is the authoritative "when" for all event types and is used to derive `UpTime`.
  */
opaque type Timestamp = ZonedDateTime
object Timestamp:
  def apply(value: ZonedDateTime): Timestamp = value
  extension (ts: Timestamp) inline def value: ZonedDateTime = ts

  given Show[Timestamp] =
    _.value.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show

  given Encoder[Timestamp] = OpaqueLift.lift[Timestamp, ZonedDateTime, Encoder]
  given Decoder[Timestamp] = OpaqueLift.lift[Timestamp, ZonedDateTime, Decoder]
end Timestamp

// ---------------- LaunchTime ----------------

/** The instant the service was first launched. This value is fixed for the lifetime of a service instance —
  * it does not reset on panic-triggered restarts. Combined with an event's `Timestamp`, it yields the
  * service's `UpTime`.
  */
opaque type LaunchTime = ZonedDateTime
object LaunchTime:
  def apply(value: ZonedDateTime): LaunchTime = value
  extension (lt: LaunchTime)
    def zoned: ZonedDateTime = lt
    def instant: Instant = lt.toInstant
    def zoneId: ZoneId = lt.getZone

    def upTime(ts: Timestamp): UpTime = UpTime(Duration.between(zoned, ts))

  given Show[LaunchTime] = _.zoned.toLocalDateTime.show
  given Encoder[LaunchTime] = OpaqueLift.lift[LaunchTime, ZonedDateTime, Encoder]
  given Decoder[LaunchTime] = OpaqueLift.lift[LaunchTime, ZonedDateTime, Decoder]
end LaunchTime

// ---------------- LogLink ----------------

/** A partial CloudWatch Logs console URL pointing to the service's log stream. The `locate` extension method
  * appends a time-window query parameter (±30 seconds around an event timestamp) so that observers can
  * deep-link directly to the relevant log entries.
  */
opaque type LogLink = String
object LogLink:
  def apply(str: String): LogLink = str

  private val window: Duration = 30.seconds.toJava
  extension (ll: LogLink)
    def locate(timestamp: Timestamp): String = {
      val start = timestamp.minus(window).toInstant.toEpochMilli
      val end = timestamp.plus(window).toInstant.toEpochMilli
      ll + s"$$3Fstart$$3D$start$$26end$$3D$end"
    }

  given Encoder[LogLink] = OpaqueLift.lift[LogLink, String, Encoder]
  given Decoder[LogLink] = OpaqueLift.lift[LogLink, String, Decoder]
end LogLink

/** Controls the minimum severity at which log messages are processed.
  *
  * The guard distinguishes two logging paths:
  *
  *   - '''logger''' writes messages to the log sink (typically the application log file or console).
  *   - '''channel''' publishes messages to the event channel (for observers, alerting, and downstream
  *     consumers).
  *
  * Each path has an independent threshold: messages below a path's threshold are discarded for that path. The
  * `min` field is the lower of the two, used by `logger.enabled` so that a message is created whenever either
  * path would accept it.
  *
  * @param logger
  *   minimum level for writing to the log sink
  * @param channel
  *   minimum level for publishing to the event channel
  */
final case class LogThreshold(logger: LogLevel, channel: LogLevel) derives Codec.AsObject {
  val min: LogLevel = logger.min(channel)
}
