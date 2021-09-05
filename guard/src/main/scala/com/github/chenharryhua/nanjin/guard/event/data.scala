package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.implicits.toShow
import com.codahale.metrics.*
import com.codahale.metrics.json.MetricsModule
import com.fasterxml.jackson.databind.ObjectMapper
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import io.circe.shapes.*
import io.circe.syntax.*
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.time.{ZoneId, ZonedDateTime}
import java.util.concurrent.TimeUnit
import java.util.{TimeZone, UUID}
import scala.collection.JavaConverters.*
import scala.collection.immutable

@JsonCodec
sealed trait NJRuntimeInfo {
  def uuid: UUID
  def launchTime: ZonedDateTime
}

final case class ServiceInfo(uuid: UUID, launchTime: ZonedDateTime) extends NJRuntimeInfo
final case class ActionInfo(uuid: UUID, launchTime: ZonedDateTime) extends NJRuntimeInfo

@JsonCodec
final case class Notes private (value: String) extends AnyVal

private[guard] object Notes {
  def apply(str: String): Notes = new Notes(Option(str).getOrElse("null in notes"))
}

final case class NJError private (
  uuid: UUID,
  message: String,
  stackTrace: String,
  throwable: Option[Throwable]
)

private[guard] object NJError {
  implicit val showNJError: Show[NJError] = ex => s"NJError(id=${ex.uuid.show}, message=${ex.message})"

  implicit val encodeNJError: Encoder[NJError] = (a: NJError) =>
    Json.obj(
      ("uuid", a.uuid.asJson),
      ("message", a.message.asJson),
      ("stackTrace", a.stackTrace.asJson)
    )

  implicit val decodeNJError: Decoder[NJError] = (c: HCursor) =>
    for {
      id <- c.downField("uuid").as[UUID]
      msg <- c.downField("message").as[String]
      st <- c.downField("stackTrace").as[String]
    } yield NJError(id, msg, st, None) // can not reconstruct throwables.

  def apply(ex: Throwable): NJError =
    NJError(UUID.randomUUID(), ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex), Some(ex))
}

sealed trait RunMode extends EnumEntry
object RunMode extends Enum[RunMode] with CatsEnum[RunMode] with CirceEnum[RunMode] {
  override val values: immutable.IndexedSeq[RunMode] = findValues
  case object Parallel extends RunMode
  case object Sequential extends RunMode
}

@JsonCodec
final case class MetricsSnapshot private (counters: Map[String, Long], text: String, asJson: Json, show: String) {
  override val toString: String = show
}

private[guard] object MetricsSnapshot {
  def apply(
    metricRegistry: MetricRegistry,
    rateTimeUnit: TimeUnit,
    durationTimeUnit: TimeUnit,
    zoneId: ZoneId): MetricsSnapshot = {
    val show: String = {
      val bao = new ByteArrayOutputStream
      val ps  = new PrintStream(bao)
      ConsoleReporter
        .forRegistry(metricRegistry)
        .convertRatesTo(rateTimeUnit)
        .convertDurationsTo(durationTimeUnit)
        .formattedFor(TimeZone.getTimeZone(zoneId))
        .outputTo(ps)
        .build()
        .report()
      ps.flush()
      ps.close()
      bao.toString(StandardCharsets.UTF_8.name())
    }

    val json: Json = {
      val str =
        new ObjectMapper()
          .registerModule(new MetricsModule(rateTimeUnit, durationTimeUnit, false))
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(metricRegistry)
      io.circe.jackson.parse(str).toOption.getOrElse(Json.Null)
    }

    val timer: Map[String, Long]   = metricRegistry.getTimers.asScala.mapValues(_.getCount).toMap
    val counter: Map[String, Long] = metricRegistry.getCounters.asScala.mapValues(_.getCount).toMap

    val text: String = (timer ++ counter).map(x => s"${x._1}: *${x._2}*").toList.sorted.mkString("\n")

    MetricsSnapshot(timer ++ counter, text, json, show)
  }

  implicit val showMetricsSnapshot: Show[MetricsSnapshot] = _.show
}
