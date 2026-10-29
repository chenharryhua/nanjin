package com.github.chenharryhua.nanjin.guard.metrics.snapshot

import cats.effect.IO
import cats.effect.std.Dispatcher
import com.codahale.metrics.{Gauge as CodahaleGauge, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{Domain, Service, Task}
import com.github.chenharryhua.nanjin.guard.metrics.{MetricScope, MetricsHub}
import io.circe.Json
import org.typelevel.otel4s.metrics.MeterProvider
import munit.CatsEffectSuite

import java.time.ZoneId
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationInt

/** Direct tests for translating the Dropwizard registry into snapshots.
  *
  * These avoid service lifecycle and reporting delays: real metrics are registered through `MetricsHub`,
  * scraped while their resources are alive, and then automatically unregistered. Raw invalid registry entries
  * cover the scraper's defensive filtering without fabricating `MetricId` values.
  */
class ScrapeMetricsTest extends CatsEffectSuite {

  private val SCOPE =
    MetricScope(MetricScope.Label("scrape"), Domain("test"), Service("metrics"), Task("snapshot"))

  private def with_hub[A](run: (MetricsHub[IO], MetricRegistry, ScrapeMetrics) => IO[A]): IO[A] =
    Dispatcher.parallel[IO].use { dispatcher =>
      MeterProvider.noop[IO].get("scrape-test").flatMap { otel_meter =>
        val registry = new MetricRegistry
        val hub = MetricsHub[IO](SCOPE, registry, dispatcher, ZoneId.of("UTC"), otel_meter)
        run(hub, registry, new ScrapeMetrics(registry))
      }
    }

  test("1.cheap and full snapshots convert each metric family and full alone evaluates gauges") {
    with_hub { (hub, _, scraper) =>
      val metrics = for {
        zero <- hub.counter("zero")
        counter <- hub.counter("counter")
        meter <- hub.meter("meter")
        histogram <- hub.histogram("histogram")
        timer <- hub.timer("timer")
        _ <- hub.gauge("value", _.register(IO.pure(Json.fromInt(42))))
        _ <- hub.gauge("null", _.register(IO.pure(Json.Null)))
        _ <- hub.numericGauge("numeric", IO.pure(9L))
      } yield (zero, counter, meter, histogram, timer)

      metrics.use { case (_, counter, meter, histogram, timer) =>
        for {
          _ <- counter.inc(2)
          _ <- meter.mark(3)
          _ <- histogram.update(4)
          _ <- timer.elapsedNano(5)
          cheap <- scraper.snapshot[IO](ScrapeMode.Cheap)
          full <- scraper.snapshot[IO](ScrapeMode.Full)
        } yield {
          assertEquals(cheap.counters.size, 1) // the zero counter is filtered
          assertEquals(cheap.meters.size, 1)
          assertEquals(cheap.histograms.size, 1)
          assertEquals(cheap.timers.size, 1)
          assertEquals(cheap.gauges, Nil)

          assertEquals(full.counters.size, 1)
          assertEquals(full.meters.size, 1)
          assertEquals(full.histograms.size, 1)
          assertEquals(full.timers.size, 1)
          assertEquals(full.gauges.size, 2) // Json.Null is filtered; value and numeric remain
          assert(full.gauges.exists(_.gauge.value == Json.fromInt(42)))
          assert(full.gauges.exists(_.gauge.value == Json.fromLong(9L)))
        }
      }
    }
  }

  test("2.invalid identifiers and registry/category mismatches are ignored") {
    with_hub { (hub, registry, scraper) =>
      for {
        counter_id <- hub.counter("counter-id").use { _ =>
          IO(registry.getCounters.firstKey())
        }
        _ <- IO {
          registry.counter("not-json").inc(1L)
          registry.meter(counter_id).mark(1L) // valid Counter id stored under the Meter registry type
        }
        cheap <- scraper.snapshot[IO](ScrapeMode.Cheap)
      } yield {
        assertEquals(cheap.counters, Nil)
        assertEquals(cheap.meters, Nil)
      }
    }
  }

  test("3.full snapshot ignores malformed gauge JSON under an otherwise valid metric id") {
    with_hub { (hub, registry, scraper) =>
      for {
        gauge_id <- hub.gauge("gauge-id", _.register(IO.pure(Json.fromInt(1)))).use { _ =>
          IO(registry.getGauges.firstKey())
        }
        _ <- IO {
          registry.register(
            gauge_id,
            new CodahaleGauge[String] {
              override def getValue: String = "not-json"
            })
        }
        full <- scraper.snapshot[IO](ScrapeMode.Full)
      } yield assertEquals(full.gauges, Nil)
    }
  }

  test("4.numeric gauge failures and timeouts remain visible as error JSON") {
    with_hub { (hub, _, scraper) =>
      val gauges = for {
        _ <- hub.numericGauge(
          "failure",
          IO.raiseError[Long](new RuntimeException("broken")),
          _.withTimeout(100.millis))
        _ <- hub.numericGauge("timeout", IO.never[Long], _.withTimeout(20.millis))
      } yield ()

      gauges.use { _ =>
        scraper.snapshot[IO](ScrapeMode.Full).map { full =>
          assertEquals(full.gauges.size, 2)
          assert(full.gauges.forall(!_.gauge.value.isNull))
        }
      }
    }
  }

  test("5.meteredCounts includes meters and timers and ignores invalid registry names") {
    with_hub { (hub, registry, scraper) =>
      val metrics = for {
        meter <- hub.meter("meter")
        timer <- hub.timer("timer")
      } yield (meter, timer)

      metrics.use { case (meter, timer) =>
        for {
          _ <- meter.mark(3)
          _ <- timer.elapsedNano(1)
          _ <- IO(registry.meter("invalid-meter-id").mark(99L))
          counts <- scraper.meteredCounts[IO]
        } yield assertEquals(counts.values.toSet, Set(3L, 1L))
      }
    }
  }

  test("6.valid metric ids stored under the wrong registry family are ignored") {
    with_hub { (hub, registry, scraper) =>
      def counter_id(name: String): IO[String] =
        hub.counter(name).use(_ => IO(registry.getCounters.firstKey()))

      for {
        histogram_id <- counter_id("as-histogram")
        timer_id <- counter_id("as-timer")
        gauge_id <- counter_id("as-gauge")
        metered_id <- counter_id("as-metered")
        _ <- IO {
          registry.histogram(histogram_id).update(1)
          registry.timer(timer_id).update(1L, TimeUnit.NANOSECONDS)
          registry.register(
            gauge_id,
            new CodahaleGauge[String] {
              override def getValue: String = "1"
            })
          registry.meter(metered_id).mark(1L)
        }
        full <- scraper.snapshot[IO](ScrapeMode.Full)
        counts <- scraper.meteredCounts[IO]
      } yield {
        assertEquals(full.histograms, Nil)
        assertEquals(full.timers, Nil)
        assertEquals(full.gauges, Nil)
        assertEquals(counts, Map.empty)
      }
    }
  }
}
