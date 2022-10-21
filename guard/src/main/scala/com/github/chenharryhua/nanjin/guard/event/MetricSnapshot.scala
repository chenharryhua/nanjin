package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import cats.implicits.catsSyntaxSemigroup
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
final case class MetricSnapshot(counterMap: Map[String, Long], asJson: Json, show: String) {
  override def toString: String = show
}

object MetricSnapshot {

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

  def apply(
    metricRegistry: MetricRegistry,
    serviceParams: ServiceParams,
    filter: MetricFilter): MetricSnapshot =
    MetricSnapshot(
      counters(metricRegistry, filter),
      toJson(
        metricRegistry,
        filter |+| positiveFilter,
        serviceParams.metricParams.rateTimeUnit,
        serviceParams.metricParams.durationTimeUnit),
      toText(
        metricRegistry,
        filter |+| positiveFilter,
        serviceParams.metricParams.rateTimeUnit,
        serviceParams.metricParams.durationTimeUnit,
        serviceParams.taskParams.zoneId)
    )
}
