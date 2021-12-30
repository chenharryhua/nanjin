package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.implicits.{catsSyntaxEq, toShow}
import com.codahale.metrics.*
import com.codahale.metrics.json.MetricsModule
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.chenharryhua.nanjin.datetime.instances.*
import io.circe.Json
import io.circe.generic.JsonCodec

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.time.ZoneId
import java.util.TimeZone
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

@JsonCodec
sealed trait MetricSnapshot {
  def counterMap: Map[String, Long]
  def asJson: Json
  def show: String
  final override def toString: String = show
}

object MetricSnapshot {

  implicit val showSnapshot: Show[MetricSnapshot] = _.show

  private def toText(
    metricRegistry: MetricRegistry,
    metricFilter: MetricFilter,
    rateTimeUnit: TimeUnit,
    durationTimeUnit: TimeUnit,
    zoneId: ZoneId): String = {
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

  private def toJson(
    metricRegistry: MetricRegistry,
    metricFilter: MetricFilter,
    rateTimeUnit: TimeUnit,
    durationTimeUnit: TimeUnit): Json = {
    val str =
      new ObjectMapper()
        .registerModule(new MetricsModule(rateTimeUnit, durationTimeUnit, false, metricFilter))
        .writerWithDefaultPrettyPrinter()
        .writeValueAsString(metricRegistry)
    io.circe.jackson.parse(str).fold(_ => Json.Null, identity)
  }

  private def counters(metricRegistry: MetricRegistry, metricFilter: MetricFilter): Map[String, Long] =
    metricRegistry.getCounters(metricFilter).asScala.view.mapValues(_.getCount).toMap

  private def meters(metricRegistry: MetricRegistry, metricFilter: MetricFilter): Map[String, Long] =
    metricRegistry.getMeters(metricFilter).asScala.view.mapValues(_.getCount).toMap

  private def timers(metricRegistry: MetricRegistry, metricFilter: MetricFilter): Map[String, Long] =
    metricRegistry.getTimers(metricFilter).asScala.view.mapValues(_.getCount).toMap

  private def histograms(metricRegistry: MetricRegistry, metricFilter: MetricFilter): Map[String, Long] =
    metricRegistry.getHistograms(metricFilter).asScala.view.mapValues(_.getCount).toMap

  final case class Last private ( // not a snapshot
    counterCount: Map[String, Long],
    meterCount: Map[String, Long],
    timerCount: Map[String, Long],
    histoCount: Map[String, Long])

  object Last {
    val empty: Last = Last(Map.empty, Map.empty, Map.empty, Map.empty)

    def apply(metricRegistry: MetricRegistry): Last = {
      val filter = MetricFilter.ALL
      Last(
        counterCount = counters(metricRegistry, filter),
        meterCount = meters(metricRegistry, filter),
        timerCount = timers(metricRegistry, filter),
        histoCount = histograms(metricRegistry, filter)
      )
    }
  }

  @JsonCodec
  final case class Full private (counterMap: Map[String, Long], asJson: Json, show: String) extends MetricSnapshot

  object Full {
    def apply(
      metricRegistry: MetricRegistry,
      rateTimeUnit: TimeUnit,
      durationTimeUnit: TimeUnit,
      zoneId: ZoneId): Full = {
      val metricFilter = MetricFilter.ALL
      Full(
        counters(metricRegistry, metricFilter) ++ meters(metricRegistry, metricFilter),
        toJson(metricRegistry, metricFilter, rateTimeUnit, durationTimeUnit),
        toText(metricRegistry, metricFilter, rateTimeUnit, durationTimeUnit, zoneId)
      )
    }
  }

  @JsonCodec
  final case class Adhoc private (counterMap: Map[String, Long], asJson: Json, show: String) extends MetricSnapshot

  object Adhoc {
    def apply(
      metricRegistry: MetricRegistry,
      metricFilter: MetricFilter,
      rateTimeUnit: TimeUnit,
      durationTimeUnit: TimeUnit,
      zoneId: ZoneId): Adhoc = Adhoc(
      counters(metricRegistry, metricFilter) ++ meters(metricRegistry, metricFilter),
      toJson(metricRegistry, metricFilter, rateTimeUnit, durationTimeUnit),
      toText(metricRegistry, metricFilter, rateTimeUnit, durationTimeUnit, zoneId)
    )
  }

  @JsonCodec
  final case class Delta private (counterMap: Map[String, Long], asJson: Json, show: String) extends MetricSnapshot

  object Delta {
    def apply(
      last: Last,
      metricRegistry: MetricRegistry,
      metricFilter: MetricFilter,
      rateTimeUnit: TimeUnit,
      durationTimeUnit: TimeUnit,
      zoneId: ZoneId
    ): Delta = {
      val filter: MetricFilter = (name: String, metric: Metric) =>
        metric match {
          case c: Counter =>
            last.counterCount.get(name).forall(_ =!= c.getCount) && metricFilter.matches(name, metric)
          case m: Meter =>
            last.meterCount.get(name).forall(_ =!= m.getCount) && metricFilter.matches(name, metric)
          case t: Timer =>
            last.timerCount.get(name).forall(_ =!= t.getCount) && metricFilter.matches(name, metric)
          case h: Histogram =>
            last.histoCount.get(name).forall(_ =!= h.getCount) && metricFilter.matches(name, metric)
          case _ => metricFilter.matches(name, metric)
        }

      Delta(
        counters(metricRegistry, filter) ++ meters(metricRegistry, filter),
        toJson(metricRegistry, filter, rateTimeUnit, durationTimeUnit),
        toText(metricRegistry, filter, rateTimeUnit, durationTimeUnit, zoneId)
      )
    }
  }
}
