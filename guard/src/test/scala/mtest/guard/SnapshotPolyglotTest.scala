package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.event.{
  Category,
  CounterKind,
  Domain,
  GaugeKind,
  HistogramKind,
  MeterKind,
  MetricElement,
  MetricID,
  MetricLabel,
  MetricName,
  Snapshot,
  Squants,
  TimerKind
}
import com.github.chenharryhua.nanjin.guard.translator.SnapshotPolyglot
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite
import squants.Each
import squants.time.Hertz

import java.time.Duration

class SnapshotPolyglotTest extends AnyFunSuite {
  private val label = MetricLabel("service", Domain("guard"))

  private def metricName(name: String): MetricName = MetricName[IO](name).unsafeRunSync()

  private def id(name: String, category: Category): MetricID =
    MetricID(label, metricName(name), category)

  private val snapshot: Snapshot = Snapshot(
    counters = List(MetricElement.Counter(id("requests", Category.Counter(CounterKind.Counter)), MetricElement.CounterData(42))),
    meters = List(
      MetricElement.Meter(
        id("throughput", Category.Meter(MeterKind.Meter, Squants(Each))),
        MetricElement.MeterData(
          squants = Squants(Each),
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
        id("latency", Category.Timer(TimerKind.Timer)),
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
        id("samples", Category.Histogram(HistogramKind.Histogram, Squants(Each))),
        MetricElement.HistogramData(
          squants = Squants(Each),
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
    gauges = List(MetricElement.Gauge(id("status", Category.Gauge(GaugeKind.Gauge)), MetricElement.GaugeData(Json.fromString("ok"))))
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
