package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.implicits.{catsSyntaxEq, toShow}
import com.codahale.metrics.*
import com.codahale.metrics.json.MetricsModule
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, ServiceParams}
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
import scala.jdk.CollectionConverters.*

@JsonCodec
sealed trait NJRuntimeInfo {
  def serviceParams: ServiceParams
  def uuid: UUID
  def launchTime: ZonedDateTime
}

final case class ServiceInfo(serviceParams: ServiceParams, uuid: UUID, launchTime: ZonedDateTime) extends NJRuntimeInfo
final case class ActionInfo(actionParams: ActionParams, serviceInfo: ServiceInfo, uuid: UUID, launchTime: ZonedDateTime)
    extends NJRuntimeInfo {
  override val serviceParams: ServiceParams = serviceInfo.serviceParams
}

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

  def apply(uuid: UUID, ex: Throwable): NJError =
    NJError(uuid, ExceptionUtils.getMessage(ex), ExceptionUtils.getStackTrace(ex), Some(ex))
}

@JsonCodec
final case class MetricsSnapshot private (
  counterCount: Map[String, Long],
  meterCount: Map[String, Long],
  timerCount: Map[String, Long],
  asJson: Json,
  show: String) {
  override val toString: String = show
  def isContainErrors: Boolean  = counterCount.filter(_._2 > 0).keys.exists(_.startsWith("0"))
}

private[guard] object MetricsSnapshot {
  private def create(
    metricRegistry: MetricRegistry,
    metricFilter: MetricFilter,
    rateTimeUnit: TimeUnit,
    durationTimeUnit: TimeUnit,
    zoneId: ZoneId): MetricsSnapshot = {

    val text: String = {
      val bao = new ByteArrayOutputStream
      val ps  = new PrintStream(bao)
      ConsoleReporter
        .forRegistry(metricRegistry)
        .convertRatesTo(rateTimeUnit)
        .convertDurationsTo(durationTimeUnit)
        .formattedFor(TimeZone.getTimeZone(zoneId))
        .filter(metricFilter)
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
          .registerModule(new MetricsModule(rateTimeUnit, durationTimeUnit, false, metricFilter))
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(metricRegistry)
      io.circe.jackson.parse(str).fold(_ => Json.Null, identity)
    }

    val counters: Map[String, Long] =
      metricRegistry.getCounters(metricFilter).asScala.view.mapValues(_.getCount).toMap

    val meters: Map[String, Long] =
      metricRegistry.getMeters(metricFilter).asScala.view.mapValues(_.getCount).toMap

    val timers: Map[String, Long] =
      metricRegistry.getTimers(metricFilter).asScala.view.mapValues(_.getCount).toMap

    MetricsSnapshot(counterCount = counters, meterCount = meters, timerCount = timers, json, text)
  }

  def apply(metricRegistry: MetricRegistry, metricFilter: MetricFilter, params: ServiceParams): MetricsSnapshot =
    create(
      metricRegistry = metricRegistry,
      metricFilter = metricFilter,
      rateTimeUnit = params.metric.rateTimeUnit,
      durationTimeUnit = params.metric.durationTimeUnit,
      zoneId = params.taskParams.zoneId
    )

  implicit val showMetricsSnapshot: Show[MetricsSnapshot] = _.show
}

@JsonCodec
sealed trait MetricResetType
object MetricResetType {
  implicit val showMetricResetType: Show[MetricResetType] = {
    case AdventiveReset       => "Metrics Adventive Reset"
    case ScheduledReset(next) => s"Metrics Scheduled Reset(next=${next.show})"
  }
  case object AdventiveReset extends MetricResetType
  final case class ScheduledReset(next: ZonedDateTime) extends MetricResetType
}

@JsonCodec
sealed trait MetricReportType {
  def isShow: Boolean
}

object MetricReportType {
  implicit val showMetricReportType: Show[MetricReportType] = {
    case AdventiveReport        => "Metrics Adventive Report"
    case ScheduledReport(index) => s"Metrics Scheduled Report(index=$index)"
  }
  case object AdventiveReport extends MetricReportType {
    override val isShow: Boolean = true
  }
  final case class ScheduledReport(index: Long) extends MetricReportType {
    override val isShow: Boolean = index === 0
  }
}
