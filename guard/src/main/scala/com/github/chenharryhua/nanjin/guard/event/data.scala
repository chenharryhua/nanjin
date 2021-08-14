package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.implicits.toShow
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.json.MetricsModule
import com.fasterxml.jackson.databind.ObjectMapper
import enumeratum.EnumEntry.Lowercase
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import io.circe.generic.auto.*
import io.circe.shapes.*
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.ZonedDateTime
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters.*
import scala.collection.immutable

final case class ServiceInfo(id: UUID, launchTime: ZonedDateTime)

final case class ActionInfo(actionName: String, id: UUID, launchTime: ZonedDateTime, serviceInfo: ServiceInfo)

final case class Notes private (value: String)

object Notes {
  def apply(str: String): Notes = new Notes(Option(str).getOrElse("null in notes"))
}

final case class NJError private (
  id: UUID,
  severity: Importance,
  message: String,
  stackTrace: String,
  throwable: Option[Throwable]
)

object NJError {
  implicit val showNJError: Show[NJError] = ex =>
    s"NJError(id=${ex.id}, severity=${ex.severity.show}, message=${ex.message})"

  implicit val encodeNJError: Encoder[NJError] = (a: NJError) =>
    Json.obj(
      ("id", Json.fromString(a.id.toString)),
      ("severity", Json.fromString(a.severity.show)),
      ("message", Json.fromString(a.message)),
      ("stackTrace", Json.fromString(a.stackTrace))
    )

  implicit val decodeNJError: Decoder[NJError] = (c: HCursor) =>
    for {
      id <- c.downField("id").as[UUID]
      sv <- c.downField("severity").as[Importance]
      msg <- c.downField("message").as[String]
      st <- c.downField("stackTrace").as[String]
    } yield NJError(id, sv, msg, st, None) // can not reconstruct throwables.

  def apply(ex: Throwable, severity: Importance): NJError =
    NJError(UUID.randomUUID(), severity, ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex), Some(ex))
}

final case class MetricRegistryWrapper(value: Option[MetricRegistry]) extends AnyVal

object MetricRegistryWrapper {
  implicit val showMetricRegistryWrapper: Show[MetricRegistryWrapper] =
    _.value.fold("") { mr =>
      val timer = mr.getTimers.asScala.map { case (s, t) =>
        s"Timer(name=$s, count=${t.getCount}, mean=${t.getMeanRate}}, median=${t.getSnapshot.getMedian})"
      }.toList
      val gauge =
        mr.getGauges.asScala.mapValues(_.getValue.toString).map { case (s, t) => s"Gauge(name=$s, value=$t)" }.toList
      val counter =
        mr.getCounters.asScala.mapValues(_.getCount).map { case (s, t) => s"Counter(name=$s, count=$t)" }.toList
      val histogram = mr.getHistograms.asScala.map { case (s, t) =>
        s"Histogram(name=$s, count=${t.getCount}, mean=${t.getSnapshot.getMean})"
      }.toList
      val meters = mr.getMeters.asScala.map { case (s, t) =>
        s"Meter(name=$s, count=${t.getCount}, mean=${t.getMeanRate})"
      }.toList
      (timer ::: gauge ::: counter ::: histogram ::: meters).mkString("\n")
    }

  import io.circe.jackson.parse
  implicit val encodeMetricRegistryWrapper: Encoder[MetricRegistryWrapper] =
    _.value.flatMap { mr =>
      val str =
        new ObjectMapper()
          .registerModule(new MetricsModule(TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS, false))
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(mr)
      parse(str).toOption
    }.getOrElse(Json.Null)

  implicit val decodeMetricRegistryWrapper: Decoder[MetricRegistryWrapper] =
    (c: HCursor) => Right(MetricRegistryWrapper(None))
}

sealed trait RunMode extends EnumEntry
object RunMode extends Enum[RunMode] with CatsEnum[RunMode] with CirceEnum[RunMode] {
  override val values: immutable.IndexedSeq[RunMode] = findValues
  case object Parallel extends RunMode
  case object Sequential extends RunMode
}

sealed abstract class Importance(val value: Int) extends EnumEntry with Lowercase

object Importance extends CatsEnum[Importance] with Enum[Importance] with CirceEnum[Importance] {
  override def values: immutable.IndexedSeq[Importance] = findValues

  case object SystemEvent extends Importance(0)
  case object High extends Importance(1)
  case object Medium extends Importance(2)
  case object Low extends Importance(3)
}
