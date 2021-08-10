package com.github.chenharryhua.nanjin.guard.alert

import cats.Show
import com.codahale.metrics.MetricRegistry
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import io.circe.generic.auto.*
import io.circe.shapes.*
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.ZonedDateTime
import java.util.UUID
import scala.collection.JavaConverters.*
import scala.collection.immutable

final case class ServiceInfo(id: UUID, launchTime: ZonedDateTime)

final case class ActionInfo(id: UUID, launchTime: ZonedDateTime, actionName: String, serviceInfo: ServiceInfo)

final case class Notes private (value: String)

object Notes {
  def apply(str: String): Notes = new Notes(Option(str).getOrElse("null in notes"))
}

final case class NJError private (
  id: UUID,
  message: String,
  stackTrace: String,
  throwable: Throwable,
  severity: FailureSeverity)

object NJError {
  implicit val showNJError: Show[NJError] = ex => s"NJError(id=${ex.id}, message=${ex.message})"

  implicit val encodeNJError: Encoder[NJError] = (a: NJError) =>
    Json.obj(
      ("id", Json.fromString(a.id.toString)),
      ("message", Json.fromString(a.message)),
      ("stackTrace", Json.fromString(a.stackTrace)),
      ("severity", Json.fromString(a.severity.entryName))
    )

  implicit val decodeNJError: Decoder[NJError] = (c: HCursor) =>
    for {
      id <- c.downField("id").as[UUID]
      msg <- c.downField("message").as[String]
      st <- c.downField("stackTrace").as[String]
      sv <- c.downField("severity").as[FailureSeverity]
    } yield NJError(id, msg, st, new Throwable("fake Throwable"), sv) // can not recover throwables.

  def apply(ex: Throwable, severity: FailureSeverity): NJError =
    NJError(UUID.randomUUID(), ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex), ex, severity)
}

final case class DailySummaries private (value: String)

object DailySummaries {
  def apply(registry: MetricRegistry): DailySummaries = {
    val timer   = registry.getTimers.asScala.map { case (s, t) => s"$s: *${t.getCount}*" }.toList
    val counter = registry.getCounters.asScala.map { case (s, c) => s"$s: *${c.getCount}*" }.toList
    val all     = (timer ::: counter).sorted.mkString("\n")
    DailySummaries(all)
  }
}

sealed trait RunMode extends EnumEntry
object RunMode extends Enum[RunMode] with CatsEnum[RunMode] with CirceEnum[RunMode] {
  override val values: immutable.IndexedSeq[RunMode] = findValues
  case object Parallel extends RunMode
  case object Sequential extends RunMode
}

// https://en.wikipedia.org/wiki/Syslog
sealed abstract class FailureSeverity(val value: Int) extends EnumEntry with Ordered[FailureSeverity] {
  final override def compare(that: FailureSeverity): Int = Integer.compare(value, that.value)
}

object FailureSeverity extends Enum[FailureSeverity] with CatsEnum[FailureSeverity] with CirceEnum[FailureSeverity] {
  override def values: immutable.IndexedSeq[FailureSeverity] = findValues
  case object Emergency extends FailureSeverity(0)
  case object Alert extends FailureSeverity(1)
  case object Critical extends FailureSeverity(2)
  case object Error extends FailureSeverity(3)
  case object Warning extends FailureSeverity(4)
  case object Notice extends FailureSeverity(5)
  case object Informational extends FailureSeverity(6)
  case object Debug extends FailureSeverity(7)
}
