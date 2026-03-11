package com.github.chenharryhua.nanjin.guard.event

import cats.effect.Unique
import cats.syntax.show.toShow
import cats.{Hash, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.guard.config.Opaque
import io.circe.{Decoder, Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.typelevel.cats.time.instances.localtime.localtimeInstances

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}
import scala.jdk.CollectionConverters.ListHasAsScala

// ---------------- StackTrace ----------------
opaque type StackTrace = List[String]
object StackTrace:
  def apply(ex: Throwable): StackTrace =
    ExceptionUtils.getRootCauseStackTraceList(ex).asScala.map(_.replace("\t", "")).toList
  extension (st: StackTrace) inline def value: List[String] = st
  given Encoder[StackTrace] = Opaque.lift[StackTrace, List[String], Encoder]
  given Decoder[StackTrace] = Opaque.lift[StackTrace, List[String], Decoder]
  given Show[StackTrace] = _.mkString("\n\t")
end StackTrace

// ---------------- Correlation ----------------
opaque type Correlation = String
object Correlation:
  private def iso(s: String): Correlation = s
  def apply(token: Unique.Token): Correlation =
    val id = Integer.toUnsignedLong(Hash[Unique.Token].hash(token))
    iso(f"$id%010d")
  extension (c: Correlation) inline def value: String = c

  given Show[Correlation] = _.value
  given Encoder[Correlation] = Opaque.lift[Correlation, String, Encoder]
  given Decoder[Correlation] = Opaque.lift[Correlation, String, Decoder]
end Correlation

// ---------------- Took ----------------
opaque type Took = Duration
object Took:
  def apply(value: Duration): Took = value
  extension (t: Took) inline def value: Duration = t

  given Show[Took] = t => DurationFormatter.defaultFormatter.format(t.value)
  given Encoder[Took] = Opaque.lift[Took, Duration, Encoder]
  given Decoder[Took] = Opaque.lift[Took, Duration, Decoder]
end Took
  

// ---------------- Active ----------------
opaque type Active = Duration
object Active:
  def apply(value: Duration): Active = value
  extension (a: Active) inline def value: Duration = a

  given Show[Active] = a => defaultFormatter.format(a.value)
  given Encoder[Active] = Opaque.lift[Active, Duration, Encoder]
  given Decoder[Active] = Opaque.lift[Active, Duration, Decoder]
end Active

// ---------------- Snooze ----------------
opaque type Snooze = Duration
object Snooze:
  def apply(value: Duration): Snooze = value
  extension (s: Snooze) inline def value: Duration = s

  given Show[Snooze] = s => defaultFormatter.format(s.value)
  given Encoder[Snooze] = Opaque.lift[Snooze, Duration, Encoder]
  given Decoder[Snooze] = Opaque.lift[Snooze, Duration, Decoder]
end Snooze

// ---------------- Timestamp ----------------
opaque type Timestamp = ZonedDateTime
object Timestamp:
  def apply(value: ZonedDateTime): Timestamp = value
  extension (t: Timestamp) inline def value: ZonedDateTime = t

  given Show[Timestamp] =
    _.value.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show

  given Encoder[Timestamp] = Opaque.lift[Timestamp, ZonedDateTime, Encoder]
  given Decoder[Timestamp] = Opaque.lift[Timestamp, ZonedDateTime, Decoder]
end Timestamp

// ---------------- Message ----------------
opaque type Message = Json
object Message:
  def apply(value: Json): Message = value
  extension (m: Message) inline def value: Json = m
  
  given Show[Message] = _.value.spaces2
  given Encoder[Message] = Opaque.lift[Message, Json, Encoder]
  given Decoder[Message] = Opaque.lift[Message, Json, Decoder]
end Message

// ---------------- Domain ----------------
opaque type Domain = String
object Domain:
  def apply(value: String): Domain = value
  extension (d: Domain) inline def value: String = d

  given Show[Domain] = _.value
  given Encoder[Domain] = Opaque.lift[Domain, String, Encoder]
  given Decoder[Domain] = Opaque.lift[Domain, String, Decoder]
end Domain

// ---------------- Label ----------------
opaque type Label = String
object Label:
  def apply(value: String): Label = value
  extension (l: Label) inline def value: String = l

  given Show[Label] = _.value
  given Encoder[Label] = Opaque.lift[Label, String, Encoder]
  given Decoder[Label] = Opaque.lift[Label, String, Decoder]
end Label
