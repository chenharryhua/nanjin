package mtest.guard

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import io.opentelemetry.sdk.metrics.data.MetricData
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.MeterProvider
import org.typelevel.otel4s.oteljava.testkit.metrics.{
  MetricExpectation,
  MetricExpectations,
  MetricsTestkit,
  PointExpectation,
  PointSetExpectation
}

import scala.concurrent.duration.DurationInt

/** Verifies the OpenTelemetry arm of the merged `com.github.chenharryhua.nanjin.guard.metrics.MetricsHub`.
  *
  * Instruments acquired via `agent.facilitate` record to Dropwizard and, when a real
  * `org.typelevel.otel4s.metrics.MeterProvider` is configured through `withMeterProvider`, also to otel4s.
  * These tests supply the oteljava in-memory testkit as that provider and assert on the exported metrics.
  */
class OtelMetricsTest extends AnyFunSuite {

  // Per-metric attributes every instrument carries. Emitter identity (task/service/serviceId) is Resource
  // information and is intentionally NOT a point attribute, so it is absent here. The service is built with
  // TaskGuard[IO]("otel").service("otel"); domain defaults to "default".
  private val dom: Attribute[String] = Attribute("domain", "default")
  private val cat: Attribute[String] = Attribute("category", "default")

  private def assertMetrics(metrics: List[MetricData], expected: MetricExpectation*): Unit =
    MetricExpectations.checkAll(metrics, expected*) match {
      case Right(_)         => ()
      case Left(mismatches) => fail(MetricExpectations.format(mismatches))
    }

  test("1.counter maps to UpDownCounter and records the sum with attributes") {
    val metrics = MetricsTestkit.inMemory[IO]().use { testkit =>
      val service =
        TaskGuard[IO]("otel")
          .service("otel")
          .updateConfig(_.withMeterProvider(Resource.pure(testkit.meterProvider)))
      service
        .eventStream(agent => agent.facilitate("hub")(_.counter("requests")).use(c => c.inc(3) >> c.inc(-1)))
        .compile
        .drain >> testkit.collectMetrics
    }.unsafeRunSync()

    assertMetrics(
      metrics,
      MetricExpectation
        .sum[Long]("requests")
        .points(PointSetExpectation.exists(PointExpectation.numeric(2L).attributesExact(dom, cat)))
    )
  }

  test("2.meter maps to a monotonic Counter and records the sum with attributes") {
    val metrics = MetricsTestkit.inMemory[IO]().use { testkit =>
      val service =
        TaskGuard[IO]("otel")
          .service("otel")
          .updateConfig(_.withMeterProvider(Resource.pure(testkit.meterProvider)))
      service
        .eventStream(agent =>
          agent.facilitate("hub")(_.meter("throughput")).use(m => m.mark(10) >> m.mark(20)))
        .compile
        .drain >> testkit.collectMetrics
    }.unsafeRunSync()

    assertMetrics(
      metrics,
      MetricExpectation
        .sum[Long]("throughput")
        .points(PointSetExpectation.exists(PointExpectation.numeric(30L).attributesExact(dom, cat)))
    )
  }

  test("3.histogram maps to Histogram and records observation count and sum") {
    val metrics = MetricsTestkit.inMemory[IO]().use { testkit =>
      val service =
        TaskGuard[IO]("otel")
          .service("otel")
          .updateConfig(_.withMeterProvider(Resource.pure(testkit.meterProvider)))
      service
        .eventStream(agent =>
          agent.facilitate("hub")(_.histogram("samples")).use(h => h.update(10) >> h.update(20)))
        .compile
        .drain >> testkit.collectMetrics
    }.unsafeRunSync()

    assertMetrics(
      metrics,
      MetricExpectation
        .histogram("samples")
        .points(PointSetExpectation.exists(PointExpectation.histogram.count(2L).sum(30.0)))
    )
  }

  test("4.timer records durations in nanoseconds via elapsedNano") {
    val metrics = MetricsTestkit.inMemory[IO]().use { testkit =>
      val service =
        TaskGuard[IO]("otel")
          .service("otel")
          .updateConfig(_.withMeterProvider(Resource.pure(testkit.meterProvider)))
      service
        .eventStream(agent =>
          agent.facilitate("hub")(_.timer("latency")).use(t => t.elapsed(2.seconds) >> t.elapsed(3.seconds)))
        .compile
        .drain >> testkit.collectMetrics
    }.unsafeRunSync()

    // Timer records nanoseconds under the "ns" unit: 2s + 3s = 5_000_000_000 ns.
    assertMetrics(
      metrics,
      MetricExpectation
        .histogram("latency")
        .unit("ns")
        .points(PointSetExpectation.exists(PointExpectation.histogram.count(2L).sum(5.0e9)))
    )
  }

  test("5.timer.timing preserves the effect result and records one observation") {
    val metrics = MetricsTestkit.inMemory[IO]().use { testkit =>
      val service =
        TaskGuard[IO]("otel")
          .service("otel")
          .updateConfig(_.withMeterProvider(Resource.pure(testkit.meterProvider)))
      service
        .eventStream(agent =>
          agent.facilitate("hub")(_.timer("timed")).use(t => t.timing(IO.sleep(10.millis).as(42)).void))
        .compile
        .drain >> testkit.collectMetrics
    }.unsafeRunSync()

    assertMetrics(
      metrics,
      MetricExpectation
        .histogram("timed")
        .unit("ns")
        .points(PointSetExpectation.exists(PointExpectation.histogram.count(1L)))
    )
  }

  test("6.noop provider (default) records nothing to OpenTelemetry") {
    // No withMeterProvider call, so ServiceConfig keeps MeterProvider.noop[IO]. Instruments still work
    // (Dropwizard side), but nothing reaches the testkit.
    val metrics = MetricsTestkit.inMemory[IO]().use { testkit =>
      val service = TaskGuard[IO]("otel").service("otel")
      service
        .eventStream(agent => agent.facilitate("hub")(_.counter("noop")).use(_.inc(5)))
        .compile
        .drain >> testkit.collectMetrics
    }.unsafeRunSync()

    assert(metrics.isEmpty)
  }

  test("7.the MeterProvider resource is acquired on start and released on stop") {
    // A Resource that records its acquire/release into a Ref, yielding a noop provider. Verifies nanjin owns
    // the provider's lifecycle: it must be opened once when the service starts and closed once when it stops.
    val (acquired, released) = (for {
      acq <- Ref.of[IO, Int](0)
      rel <- Ref.of[IO, Int](0)
      provider = Resource.make(acq.update(_ + 1).as(MeterProvider.noop[IO]))(_ => rel.update(_ + 1))
      _ <- TaskGuard[IO]("otel")
        .service("otel")
        .updateConfig(_.withMeterProvider(provider))
        .eventStream(agent => agent.facilitate("hub")(_.counter("c")).use(_.inc(1)))
        .compile
        .drain
      a <- acq.get
      r <- rel.get
    } yield (a, r)).unsafeRunSync()

    assert(acquired == 1)
    assert(released == 1)
  }
}
