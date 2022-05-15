package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.implicits.{catsSyntaxEq, catsSyntaxSemigroup, toShow}
import cats.kernel.Monoid
import com.codahale.metrics.*
import com.codahale.metrics.json.MetricsModule
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import io.circe.Json
import io.circe.generic.JsonCodec

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.time.ZoneId
import java.util.TimeZone
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

@JsonCodec
final case class MetricSnapshot private (counterMap: Map[String, Long], asJson: Json, show: String) {
  override def toString: String = show
  def isContainErrors: Boolean  = counterMap.filter(_._2 > 0).keys.exists(_.startsWith("0"))
}

private[guard] object MetricSnapshot {

  implicit val monoidMetricFilter: Monoid[MetricFilter] = new Monoid[MetricFilter] {
    override val empty: MetricFilter = MetricFilter.ALL

    override def combine(x: MetricFilter, y: MetricFilter): MetricFilter =
      (name: String, metric: Metric) => x.matches(name, metric) && y.matches(name, metric)
  }

  val positiveFilter: MetricFilter =
    (_: String, metric: Metric) =>
      metric match {
        case c: Counting => c.getCount > 0
        case _           => true
      }

  def deltaFilter(lastCounters: LastCounters): MetricFilter =
    (name: String, metric: Metric) =>
      metric match {
        case c: Counter   => lastCounters.counterCount.get(name).forall(_ =!= c.getCount)
        case m: Meter     => lastCounters.meterCount.get(name).forall(_ =!= m.getCount)
        case t: Timer     => lastCounters.timerCount.get(name).forall(_ =!= t.getCount)
        case h: Histogram => lastCounters.histoCount.get(name).forall(_ =!= h.getCount)
        case _            => true
      }

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
      new ObjectMapper
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

  final case class LastCounters private (
    counterCount: Map[String, Long],
    meterCount: Map[String, Long],
    timerCount: Map[String, Long],
    histoCount: Map[String, Long])

  final object LastCounters {
    val empty: LastCounters = LastCounters(Map.empty, Map.empty, Map.empty, Map.empty)

    def apply(metricRegistry: MetricRegistry): LastCounters = {
      val filter: MetricFilter = MetricFilter.ALL
      LastCounters(
        counterCount = counters(metricRegistry, filter),
        meterCount = meters(metricRegistry, filter),
        timerCount = timers(metricRegistry, filter),
        histoCount = histograms(metricRegistry, filter)
      )
    }
  }

  private def build(
    metricRegistry: MetricRegistry,
    serviceParams: ServiceParams,
    filter: MetricFilter): MetricSnapshot =
    MetricSnapshot(
      counters(metricRegistry, filter),
      toJson(metricRegistry, filter, serviceParams.metric.rateTimeUnit, serviceParams.metric.durationTimeUnit),
      toText(
        metricRegistry,
        filter,
        serviceParams.metric.rateTimeUnit,
        serviceParams.metric.durationTimeUnit,
        serviceParams.taskParams.zoneId)
    )

  def full(metricRegistry: MetricRegistry, serviceParams: ServiceParams): MetricSnapshot =
    build(metricRegistry, serviceParams, MetricFilter.ALL)

  def regular(
    metricFilter: MetricFilter,
    metricRegistry: MetricRegistry,
    serviceParams: ServiceParams): MetricSnapshot =
    build(metricRegistry, serviceParams, metricFilter |+| positiveFilter)

  def delta(
    lastCounters: LastCounters,
    metricFilter: MetricFilter,
    metricRegistry: MetricRegistry,
    serviceParams: ServiceParams): MetricSnapshot =
    build(metricRegistry, serviceParams, metricFilter |+| positiveFilter |+| deltaFilter(lastCounters))
}
