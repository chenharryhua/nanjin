package com.github.chenharryhua.nanjin.guard.event

import cats.effect.Unique
import cats.{Hash, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter as fmt
import com.github.chenharryhua.nanjin.common.OpaqueLift
import io.circe.{Decoder, Encoder, Json}

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

// ---------------- Correlation ----------------

/** A deterministic, 10-digit correlation identifier derived from a `Unique.Token`. Used to trace a single
  * `ReportedEvent` across translators and observers. The value is stable for a given token instance, making
  * it suitable for log correlation without requiring external ID generators.
  */
opaque type Correlation = String
object Correlation:
  private def iso(s: String): Correlation = s
  def apply(token: Unique.Token): Correlation =
    val id = Integer.toUnsignedLong(Hash[Unique.Token].hash(token))
    iso(f"$id%010d")
  extension (c: Correlation) inline def value: String = c

  given Show[Correlation] = OpaqueLift.lift[Correlation, String, Show]
  given Encoder[Correlation] = OpaqueLift.lift[Correlation, String, Encoder]
  given Decoder[Correlation] = OpaqueLift.lift[Correlation, String, Decoder]
end Correlation

// ---------------- Took ----------------

/** Wall-clock duration of an operation, typically how long a metrics scrape took to complete. Displayed in
  * human-readable format via `DurationFormatter`.
  */
opaque type Took = Duration
object Took:
  def apply(value: Duration): Took = value
  def apply(fd: FiniteDuration): Took = fd.toJava
  extension (t: Took) inline def value: Duration = t

  given Show[Took] = t => fmt.format(t.value)
  given Encoder[Took] = OpaqueLift.lift[Took, Duration, Encoder]
  given Decoder[Took] = OpaqueLift.lift[Took, Duration, Decoder]
end Took

// ---------------- Active ----------------

/** The duration a service has been actively running (excluding time spent sleeping between panic-triggered
  * restarts). Contrast with `UpTime`, which measures total elapsed time since launch.
  */
opaque type Active = Duration
object Active:
  def apply(value: Duration): Active = value
  extension (a: Active) inline def value: Duration = a

  given Show[Active] = a => fmt.format(a.value)
  given Encoder[Active] = OpaqueLift.lift[Active, Duration, Encoder]
  given Decoder[Active] = OpaqueLift.lift[Active, Duration, Decoder]
end Active

// ---------------- Snooze ----------------

/** The delay between a panic and the next restart attempt, as determined by the restart policy. Corresponds
  * to the `snooze` value on a `ServicePanic` event — i.e., how long the service will sleep before retrying.
  */
opaque type Snooze = Duration
object Snooze:
  def apply(value: Duration): Snooze = value
  extension (s: Snooze) inline def value: Duration = s

  given Show[Snooze] = s => fmt.format(s.value)
  given Encoder[Snooze] = OpaqueLift.lift[Snooze, Duration, Encoder]
  given Decoder[Snooze] = OpaqueLift.lift[Snooze, Duration, Decoder]
end Snooze

// ---------------- Message ----------------

/** The JSON-encoded payload of a `ReportedEvent`, carrying the user-supplied log message. Wrapping in an
  * opaque type separates the message content from other JSON fields in the event model and provides a
  * dedicated `Show` instance for formatted display.
  */
opaque type Message = Json
object Message:
  def apply(value: Json): Message = value
  extension (m: Message) inline def value: Json = m

  given Show[Message] = _.value.spaces2
  given Encoder[Message] = OpaqueLift.lift[Message, Json, Encoder]
  given Decoder[Message] = OpaqueLift.lift[Message, Json, Decoder]
end Message
