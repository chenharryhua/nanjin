package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.implicits.toShow
import com.codahale.metrics.json.MetricsModule
import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import com.fasterxml.jackson.databind.ObjectMapper
import enumeratum.EnumEntry.Lowercase
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import io.circe.generic.auto.*
import io.circe.shapes.*
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.time.ZonedDateTime
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.immutable

final case class ServiceInfo(id: UUID, launchTime: ZonedDateTime)

final case class ActionInfo(actionName: String, id: UUID, launchTime: ZonedDateTime, serviceInfo: ServiceInfo)

final case class Notes private (value: String)

object Notes {
  def apply(str: String): Notes = new Notes(Option(str).getOrElse("null in notes"))
}

final case class NJError private (
  id: UUID,
  importance: Importance,
  message: String,
  stackTrace: String,
  throwable: Option[Throwable]
)

object NJError {
  implicit val showNJError: Show[NJError] = ex =>
    s"NJError(id=${ex.id}, severity=${ex.importance.show}, message=${ex.message})"

  implicit val encodeNJError: Encoder[NJError] = (a: NJError) =>
    Json.obj(
      ("id", Json.fromString(a.id.toString)),
      ("importance", Json.fromString(a.importance.show)),
      ("message", Json.fromString(a.message)),
      ("stackTrace", Json.fromString(a.stackTrace))
    )

  implicit val decodeNJError: Decoder[NJError] = (c: HCursor) =>
    for {
      id <- c.downField("id").as[UUID]
      sv <- c.downField("importance").as[Importance]
      msg <- c.downField("message").as[String]
      st <- c.downField("stackTrace").as[String]
    } yield NJError(id, sv, msg, st, None) // can not reconstruct throwables.

  def apply(ex: Throwable, importance: Importance): NJError =
    NJError(UUID.randomUUID(), importance, ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex), Some(ex))
}

final case class MetricRegistryWrapper(value: Option[MetricRegistry]) extends AnyVal

object MetricRegistryWrapper {
  implicit val showMetricRegistryWrapper: Show[MetricRegistryWrapper] =
    _.value.fold("") { mr =>
      val bao = new ByteArrayOutputStream
      val ps  = new PrintStream(bao)
      ConsoleReporter.forRegistry(mr).outputTo(ps).build().report()
      ps.flush()
      ps.close()
      bao.toString(StandardCharsets.UTF_8.name())
    }

  implicit val encodeMetricRegistryWrapper: Encoder[MetricRegistryWrapper] =
    _.value.flatMap { mr =>
      val str =
        new ObjectMapper()
          .registerModule(new MetricsModule(TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS, false))
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(mr)
      io.circe.jackson.parse(str).toOption
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
