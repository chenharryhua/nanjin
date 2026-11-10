package com.github.chenharryhua.nanjin.guard.translator

import com.github.chenharryhua.nanjin.guard.config.NbspChar
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.{MetricElement, Snapshot}
import com.github.chenharryhua.nanjin.guard.metrics.{MetricCategory, MetricId, Squants}
import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.FunSuite
import squants.time.{Days, Hertz, Hours, Microseconds, Milliseconds, Minutes, Nanoseconds, Seconds, Time}

import java.util.concurrent.{Callable, Executors}

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

  test("5.fractional values keep their fraction instead of rounding to a whole number") {
    // The shared presentation pattern used to be grouping-only, "#,###", which rounded every fractional
    // value: a 0.4/s rate read as "0 ea/s", a 0.5 standard deviation as "0", and a 0.85 gauge as "1".
    val id = meter_id("fractional-meter", 1L)
    val fractional_meter = MetricElement.Meter(
      id,
      MetricElement.MeterData(
        squants = meter_squants(id),
        aggregate = 7L,
        mean_rate = Hertz(2.5),
        m1_rate = Hertz(0.4),
        m5_rate = Hertz(0.25),
        m15_rate = Hertz(0.125))
    )
    // the histogram fixture carries mean = 1.5 and stddev = 0.5; "ea"/Dimensionless takes the non-time arm
    val fractional_histogram = histogram("fractional-histogram", 2L, "ea", "Dimensionless")
    val fractional_gauge =
      MetricElement.Gauge(
        gauge_id("fractional-gauge", 3L),
        MetricElement.GaugeData(Json.fromDoubleOrNull(0.85)))

    val snapshot =
      Snapshot(Nil, List(fractional_meter), Nil, List(fractional_histogram), List(fractional_gauge))
    val yaml = new SnapshotPolyglot(snapshot).toYaml

    // meter rates, which are per-second and so never adaptively rescaled
    assert(yaml.contains("0.4 ea/s"), yaml)
    assert(yaml.contains("0.25 ea/s"), yaml)
    assert(yaml.contains("0.125 ea/s"), yaml)
    assert(yaml.contains("2.5 ea/s"), yaml)
    // non-time histogram statistics
    assert(yaml.contains("1.5 ea"), yaml)
    assert(yaml.contains("0.5 ea"), yaml)
    // gauge values, which render through JsonView
    assert(yaml.contains("0.85"), yaml)
  }

  test("6.rendering the same snapshot concurrently matches rendering it alone") {
    // JsonView held its DecimalFormat in a field on an object, so every thread shared one instance.
    // DecimalFormat is mutable and not thread-safe, and the dashboard and each observer render
    // independently of one another.
    val gauges = (1 to 12).toList.map { i =>
      MetricElement.Gauge(
        gauge_id(s"gauge-$i", i.toLong),
        MetricElement.GaugeData(Json.fromDoubleOrNull(i + 0.125)))
    }
    val snapshot = Snapshot(Nil, Nil, Nil, Nil, gauges)
    val expected = new SnapshotPolyglot(snapshot).toYaml
    assert(expected.contains("0.125"), expected)

    val pool = Executors.newFixedThreadPool(16)
    try {
      val tasks = (1 to 64).map { _ =>
        pool.submit(new Callable[Int] {
          override def call(): Int =
            (1 to 40).count(_ => new SnapshotPolyglot(snapshot).toYaml != expected)
        })
      }
      assertEquals(tasks.map(_.get()).sum, 0)
    } finally pool.shutdown()
  }

  test("7.an empty-object gauge is dropped from YAML while a scalar gauge is kept") {
    // JsonView.yml renders an empty object as just the `metricName:` header, which carries no information.
    // The check has to look at the JSON, not the line count: a scalar gauge also renders to one line.
    val scalar = MetricElement.Gauge(gauge_id("scalar", 1L), MetricElement.GaugeData(Json.fromInt(42)))
    val empty_object = MetricElement.Gauge(gauge_id("hollow", 2L), MetricElement.GaugeData(Json.obj()))
    val populated =
      MetricElement.Gauge(
        gauge_id("populated", 3L),
        MetricElement.GaugeData(Json.obj("field" -> Json.fromInt(7))))

    val yaml =
      new SnapshotPolyglot(Snapshot(Nil, Nil, Nil, Nil, List(scalar, empty_object, populated))).toYaml

    assert(yaml.contains("scalar: 42"), yaml)
    assert(!yaml.contains("hollow"), yaml)
    assert(yaml.contains("populated:"), yaml)
    assert(yaml.contains("field"), yaml)
  }

  test("8.a zero mean rate renders per second rather than per day") {
    // Scaling zero never lifts it above 1, so without the zero case it fell through every branch to days
    // and an idle meter read "mean_rate: 0 ea/d", suggesting a daily rate had been measured.
    val yaml = new SnapshotPolyglot(Snapshot(Nil, List(meter("idle", 1L, 0.0)), Nil, Nil, Nil)).toYaml

    assert(yaml.contains("mean_rate: 0 ea/s"), yaml)
    assert(!yaml.contains("ea/d"), yaml)
  }

  test("9.same-named metrics in one scope collapse in JSON but not in YAML") {
    // Both renderings key a metric by token.metricName, which projects away the per-acquisition token.
    // group_json merges on that key and the oldest wins; group_yaml concatenates line blocks and keeps both.
    // Snapshot.hasDuplication detects exactly this condition.
    val older = MetricElement.Gauge(gauge_id("twin", 1L), MetricElement.GaugeData(Json.fromInt(111)))
    val newer = MetricElement.Gauge(gauge_id("twin", 2L), MetricElement.GaugeData(Json.fromInt(222)))
    val snapshot = Snapshot(Nil, Nil, Nil, Nil, List(older, newer))
    val polyglot = new SnapshotPolyglot(snapshot)

    assert(snapshot.hasDuplication)

    val pretty = polyglot.toPrettyJson.noSpaces
    assert(pretty.contains("111"), pretty)
    assert(!pretty.contains("222"), pretty)

    val yaml = polyglot.toYaml
    assert(yaml.contains("111"), yaml)
    assert(yaml.contains("222"), yaml)
  }

  test("4.empty snapshots render empty JSON arrays and empty YAML") {
    val polyglot = new SnapshotPolyglot(Snapshot.empty)
    assertEquals(polyglot.toVanillaJson, List.empty[Json].asJson)
    assertEquals(polyglot.toPrettyJson, List.empty[Json].asJson)
    assertEquals(polyglot.toYaml, "")
  }
}
