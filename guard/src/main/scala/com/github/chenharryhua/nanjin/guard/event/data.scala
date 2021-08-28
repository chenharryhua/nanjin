package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.implicits.toShow
import com.codahale.metrics.json.MetricsModule
import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import com.fasterxml.jackson.databind.ObjectMapper
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
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
import scala.collection.immutable

sealed trait NJRuntimeInfo {
  def uuid: UUID
  def launchTime: ZonedDateTime
}

final case class ServiceInfo(uuid: UUID, launchTime: ZonedDateTime) extends NJRuntimeInfo
final case class ActionInfo(uuid: UUID, launchTime: ZonedDateTime) extends NJRuntimeInfo

final case class Notes private (value: String)

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

final case class MetricRegistryWrapper(
  registry: Option[MetricRegistry],
  rateTimeUnit: TimeUnit,
  durationTimeUnit: TimeUnit,
  zoneId: ZoneId)

private[guard] object MetricRegistryWrapper {

  implicit val showMetricRegistryWrapper: Show[MetricRegistryWrapper] = { mrw =>
    mrw.registry.fold("") { mr =>
      val bao = new ByteArrayOutputStream
      val ps  = new PrintStream(bao)
      ConsoleReporter
        .forRegistry(mr)
        .convertRatesTo(mrw.rateTimeUnit)
        .convertDurationsTo(mrw.durationTimeUnit)
        .formattedFor(TimeZone.getTimeZone(mrw.zoneId))
        .outputTo(ps)
        .build()
        .report()
      ps.flush()
      ps.close()
      bao.toString(StandardCharsets.UTF_8.name())
    }
  }

  implicit val encodeMetricRegistryWrapper: Encoder[MetricRegistryWrapper] = { mrw =>
    val registry: Json = mrw.registry.flatMap { mr =>
      val str =
        new ObjectMapper()
          .registerModule(new MetricsModule(mrw.rateTimeUnit, mrw.durationTimeUnit, false))
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(mr)
      io.circe.jackson.parse(str).toOption
    }.getOrElse(Json.Null)

    Json.obj(
      "registry" -> registry,
      "rateTimeUnit" -> mrw.rateTimeUnit.asJson,
      "durationTimeUnit" -> mrw.durationTimeUnit.asJson,
      "zoneId" -> mrw.zoneId.asJson)
  }

  implicit val decodeMetricRegistryWrapper: Decoder[MetricRegistryWrapper] =
    (c: HCursor) =>
      for {
        rate <- c.downField("rateTimeUnit").as[TimeUnit]
        duration <- c.downField("durationTimeUnit").as[TimeUnit]
        tz <- c.downField("zoneId").as[ZoneId]
      } yield MetricRegistryWrapper(registry = None, rateTimeUnit = rate, durationTimeUnit = duration, zoneId = tz)
}

sealed trait RunMode extends EnumEntry
object RunMode extends Enum[RunMode] with CatsEnum[RunMode] with CirceEnum[RunMode] {
  override val values: immutable.IndexedSeq[RunMode] = findValues
  case object Parallel extends RunMode
  case object Sequential extends RunMode
}
