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
opaque type Active = Duration
object Active:
  def apply(value: Duration): Active = value
  extension (a: Active) inline def value: Duration = a

  given Show[Active] = a => fmt.format(a.value)
  given Encoder[Active] = OpaqueLift.lift[Active, Duration, Encoder]
  given Decoder[Active] = OpaqueLift.lift[Active, Duration, Decoder]
end Active

// ---------------- Snooze ----------------
opaque type Snooze = Duration
object Snooze:
  def apply(value: Duration): Snooze = value
  extension (s: Snooze) inline def value: Duration = s

  given Show[Snooze] = s => fmt.format(s.value)
  given Encoder[Snooze] = OpaqueLift.lift[Snooze, Duration, Encoder]
  given Decoder[Snooze] = OpaqueLift.lift[Snooze, Duration, Decoder]
end Snooze

// ---------------- Message ----------------
opaque type Message = Json
object Message:
  def apply(value: Json): Message = value
  extension (m: Message) inline def value: Json = m

  given Show[Message] = _.value.spaces2
  given Encoder[Message] = OpaqueLift.lift[Message, Json, Encoder]
  given Decoder[Message] = OpaqueLift.lift[Message, Json, Decoder]
end Message
