package mtest.guard

import io.circe.Json
import io.circe.jawn.parse
import org.scalatest.funsuite.AnyFunSuite
import squants.time.Hertz

import java.time.Duration
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.{MetricElement, Snapshot, SnapshotPolyglot}
import com.github.chenharryhua.nanjin.guard.metrics.{Category, MetricID}

class SnapshotPolyglotTest extends AnyFunSuite {
  private def id(name: String, category: String): MetricID =
    parse(
      s"""{
         |  "metricLabel": {"label": "service", "domain": "guard"},
         |  "metricName": {"name": "$name", "age": 0, "uniqueToken": 0},
         |  "category": $category
         |}""".stripMargin
    ).flatMap(_.as[MetricID]).fold(throw _, identity)

  private def meterSquants =
    id("throughput", meter).category match
      case Category.Meter(_, value) => value
      case _                         => throw IllegalArgumentException("expected meter category")

  private def histogramSquants =
    id("samples", histogram).category match
      case Category.Histogram(_, value) => value
      case _                            => throw IllegalArgumentException("expected histogram category")

  private val counter = """{"Counter":{"kind":{"Counter":{}}}}"""
  private val meter = """{"Meter":{"kind":{"Meter":{}},"squants":{"unitSymbol":"1","dimensionName":"Dimensionless"}}}"""
  private val timer = """{"Timer":{"kind":{"Timer":{}}}}"""
  private val histogram = """{"Histogram":{"kind":{"Histogram":{}},"squants":{"unitSymbol":"1","dimensionName":"Dimensionless"}}}"""
  private val gauge = """{"Gauge":{"kind":{"Gauge":{}}}}"""

  private val snapshot: Snapshot = Snapshot(
    counters = List(
      MetricElement.Counter(
        id("requests", counter),
        MetricElement.CounterData(42))),
    meters = List(
      MetricElement.Meter(
        id("throughput", meter),
        MetricElement.MeterData(
          squants = meterSquants,
          aggregate = 3,
          mean_rate = Hertz(1.5),
          m1_rate = Hertz(0.5),
          m5_rate = Hertz(0.25),
          m15_rate = Hertz(0.125)
        )
      )
    ),
    timers = List(
      MetricElement.Timer(
        id("latency", timer),
        MetricElement.TimerData(
          calls = 2,
          mean_rate = Hertz(2.0),
          m1_rate = Hertz(1.0),
          m5_rate = Hertz(0.5),
          m15_rate = Hertz(0.25),
          min = Duration.ofMillis(1),
          max = Duration.ofMillis(3),
          mean = Duration.ofMillis(2),
          stddev = Duration.ofMillis(1),
          p50 = Duration.ofMillis(2),
          p75 = Duration.ofMillis(2),
          p95 = Duration.ofMillis(3),
          p98 = Duration.ofMillis(3),
          p99 = Duration.ofMillis(3),
          p999 = Duration.ofMillis(3)
        )
      )
    ),
    histograms = List(
      MetricElement.Histogram(
        id("samples", histogram),
        MetricElement.HistogramData(
          squants = histogramSquants,
          updates = 4,
          min = 1,
          max = 5,
          mean = 3.0,
          stddev = 1.5,
          p50 = 3.0,
          p75 = 4.0,
          p95 = 5.0,
          p98 = 5.0,
          p99 = 5.0,
          p999 = 5.0
        )
      )
    ),
    gauges = List(
      MetricElement.Gauge(
        id("status", gauge),
        MetricElement.GaugeData(Json.fromString("ok"))))
  )

  test("renders the full snapshot across JSON and YAML formats") {
    val polyglot = new SnapshotPolyglot(snapshot)

    val prettyJson = polyglot.toPrettyJson.noSpaces
    assert(prettyJson.contains("requests"))
    assert(prettyJson.contains("throughput"))
    assert(prettyJson.contains("samples"))
    assert(prettyJson.contains("latency"))

    val vanillaJson = polyglot.toVanillaJson.noSpaces
    assert(vanillaJson.contains("requests"))
    assert(vanillaJson.contains("status"))

    val yaml = polyglot.toYaml
    assert(yaml.contains("requests:"))
    assert(yaml.contains("aggregate:"))
    assert(yaml.contains("updates:"))
    assert(yaml.contains("invocations:"))

    val slackYaml = polyglot.counterYaml.get
    assert(slackYaml.contains("requests:"))
    assert(slackYaml.contains("status:"))
  }
}
