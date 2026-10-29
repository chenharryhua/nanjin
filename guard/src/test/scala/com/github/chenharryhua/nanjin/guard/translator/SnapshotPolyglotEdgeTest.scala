package com.github.chenharryhua.nanjin.guard.translator

import com.github.chenharryhua.nanjin.guard.config.NbspChar
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.{MetricElement, Snapshot}
import com.github.chenharryhua.nanjin.guard.metrics.{MetricCategory, MetricId, Squants}
import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.FunSuite
import squants.time.{Days, Hertz, Hours, Microseconds, Milliseconds, Minutes, Nanoseconds, Seconds, Time}

class SnapshotPolyglotEdgeTest extends FunSuite {

  private def metric_id(name: String, age: Long, category: Json): MetricId =
    Json
      .obj(
        "scope" -> Json.obj(
          "label" -> Json.fromString("edge"),
          "domain" -> Json.fromString("test"),
          "service" -> Json.fromString("translator"),
          "task" -> Json.fromString("snapshot")),
        "token" -> Json.obj(
          "metricName" -> Json.fromString(name),
          "age" -> Json.fromLong(age),
          "uniqueToken" -> Json.fromInt(age.toInt)),
        "category" -> category
      )
      .as[MetricId]
      .fold(error => fail(s"invalid MetricId fixture: $error"), identity)

  private def squants_json(symbol: String, dimension: String): Json =
    Json.obj("unitSymbol" -> Json.fromString(symbol), "dimensionName" -> Json.fromString(dimension))

  private def meter_id(name: String, age: Long): MetricId =
    metric_id(
      name,
      age,
      Json.obj(
        "Meter" -> Json.obj(
          "kind" -> Json.obj("Default" -> Json.obj()),
          "squants" -> squants_json("ea", "Dimensionless"))))

  private def histogram_id(name: String, age: Long, symbol: String, dimension: String): MetricId =
    metric_id(
      name,
      age,
      Json.obj(
        "Histogram" -> Json.obj(
          "kind" -> Json.obj("Default" -> Json.obj()),
          "squants" -> squants_json(symbol, dimension))))

  private def gauge_id(name: String, age: Long): MetricId =
    metric_id(
      name,
      age,
      Json.obj(
        "Gauge" -> Json.obj(
          "kind" -> Json.obj("Default" -> Json.obj()),
          "isCached" -> Json.fromBoolean(false))))

  private def meter_squants(id: MetricId): Squants = id.category match {
    case MetricCategory.Meter(_, squants) => squants
    case other                            => fail(s"expected Meter category, got $other")
  }

  private def histogram_squants(id: MetricId): Squants = id.category match {
    case MetricCategory.Histogram(_, squants) => squants
    case other                                => fail(s"expected Histogram category, got $other")
  }

  private def meter(name: String, age: Long, mean_rate: Double): MetricElement.Meter = {
    val id = meter_id(name, age)
    MetricElement.Meter(
      id,
      MetricElement.MeterData(
        squants = meter_squants(id),
        aggregate = 1L,
        mean_rate = Hertz(mean_rate),
        m1_rate = Hertz(0.0),
        m5_rate = Hertz(0.0),
        m15_rate = Hertz(0.0))
    )
  }

  private def histogram(
    name: String,
    age: Long,
    symbol: String,
    dimension: String): MetricElement.Histogram = {
    val id = histogram_id(name, age, symbol, dimension)
    MetricElement.Histogram(
      id,
      MetricElement.HistogramData(
        squants = histogram_squants(id),
        updates = 1L,
        min = 1L,
        max = 2L,
        mean = 1.5,
        stddev = 0.5,
        p50 = 1.0,
        p75 = 1.0,
        p95 = 2.0,
        p98 = 2.0,
        p99 = 2.0,
        p999 = 2.0)
    )
  }

  test("1.slow meter rates adapt from seconds through minutes, hours, and days") {
    val snapshot = Snapshot(
      counters = Nil,
      meters = List(
        meter("per-second", 1L, 2.0),
        meter("per-minute", 2L, 0.5),
        meter("per-hour", 3L, 0.01),
        meter("per-day", 4L, 0.00001)),
      timers = Nil,
      histograms = Nil,
      gauges = Nil
    )

    val rendered = new SnapshotPolyglot(snapshot).toYaml
    assert(rendered.contains("ea/s"), rendered)
    assert(rendered.contains("ea/min"), rendered)
    assert(rendered.contains("ea/h"), rendered)
    assert(rendered.contains("ea/d"), rendered)
  }

  test("2.histograms render every supported time unit and diagnose an unknown time symbol") {
    val symbols = List(
      "nanoseconds" -> Nanoseconds.symbol,
      "microseconds" -> Microseconds.symbol,
      "milliseconds" -> Milliseconds.symbol,
      "seconds" -> Seconds.symbol,
      "minutes" -> Minutes.symbol,
      "hours" -> Hours.symbol,
      "days" -> Days.symbol
    )
    val time_histograms = symbols.zipWithIndex.map { case ((name, symbol), index) =>
      histogram(name, index.toLong, symbol, Time.name)
    }
    val snapshot = Snapshot(
      counters = Nil,
      meters = Nil,
      timers = Nil,
      histograms = time_histograms ::: List(
        histogram("unknown", 20L, "fortnight", Time.name),
        histogram("dimensionless", 21L, "ea", "Dimensionless")),
      gauges = Nil
    )

    val polyglot = new SnapshotPolyglot(snapshot)
    val yaml = polyglot.toYaml
    val json = polyglot.toPrettyJson.noSpaces

    symbols.foreach { case (name, _) =>
      assert(yaml.contains(name), yaml)
      assert(json.contains(name), json)
    }
    assert(yaml.contains("fortnight - unknown symbol of dimension"), yaml)
    assert(yaml.contains("dimensionless"), yaml)
  }

  test("3.null gauges are retained in vanilla JSON but omitted from pretty JSON and YAML") {
    val visible = MetricElement.Gauge(gauge_id("visible", 1L), MetricElement.GaugeData(Json.fromInt(1)))
    val absent = MetricElement.Gauge(gauge_id("absent", 2L), MetricElement.GaugeData(Json.Null))
    val snapshot = Snapshot(Nil, Nil, Nil, Nil, List(visible, absent))
    val polyglot = new SnapshotPolyglot(snapshot, IndentSpace.Nbsp)

    val vanilla = polyglot.toVanillaJson.noSpaces
    val pretty = polyglot.toPrettyJson.noSpaces
    val yaml = polyglot.toYaml

    assert(vanilla.contains("visible"))
    assert(vanilla.contains("absent"))
    assert(pretty.contains("visible"))
    assert(!pretty.contains("absent"))
    assert(yaml.contains("visible"))
    assert(!yaml.contains("absent"))
    assert(yaml.contains(NbspChar))
  }

  test("4.empty snapshots render empty JSON arrays and empty YAML") {
    val polyglot = new SnapshotPolyglot(Snapshot.empty)
    assertEquals(polyglot.toVanillaJson, List.empty[Json].asJson)
    assertEquals(polyglot.toPrettyJson, List.empty[Json].asJson)
    assertEquals(polyglot.toYaml, "")
  }
}
