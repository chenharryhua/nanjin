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

  // Per-metric attributes every instrument carries. These are nanjin's own conceptual grouping keys, so they
  // are namespaced with an "nj." prefix to stay distinct from any OpenTelemetry SDK Resource attributes the
  // caller sets at a higher level. The service is built with TaskGuard[IO]("otel").service("otel"), so both
  // nj.task and nj.service are "otel"; domain defaults to "default".
  private val njDomain: Attribute[String] = Attribute("nj.domain", "default")
  private val njService: Attribute[String] = Attribute("nj.service", "otel")
  private val njTask: Attribute[String] = Attribute("nj.task", "otel")

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
        .points(PointSetExpectation.exists(
          PointExpectation.numeric(2L).attributesExact(njDomain, njService, njTask)))
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
        .points(PointSetExpectation.exists(
          PointExpectation.numeric(30L).attributesExact(njDomain, njService, njTask)))
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

  test("4.timer records durations in seconds (default) via elapsedNano") {
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

    // Default time unit is seconds: 2s + 3s = 5.0 s.
    assertMetrics(
      metrics,
      MetricExpectation
        .histogram("latency")
        .unit("s")
        .points(PointSetExpectation.exists(PointExpectation.histogram.count(2L).sum(5.0)))
    )
  }

  test("4b.timer records in the configured time unit via withTimeUnit") {
    val metrics = MetricsTestkit.inMemory[IO]().use { testkit =>
      val service =
        TaskGuard[IO]("otel")
          .service("otel")
          .updateConfig(_.withMeterProvider(Resource.pure(testkit.meterProvider)))
      service
        .eventStream(agent =>
          agent
            .facilitate("hub")(_.timer("latency", _.withTimeUnit(squants.time.Milliseconds)))
            .use(t => t.elapsed(2.seconds) >> t.elapsed(3.seconds)))
        .compile
        .drain >> testkit.collectMetrics
    }.unsafeRunSync()

    // withTimeUnit(Milliseconds): 2s + 3s = 5000 ms, unit symbol "ms".
    assertMetrics(
      metrics,
      MetricExpectation
        .histogram("latency")
        .unit("ms")
        .points(PointSetExpectation.exists(PointExpectation.histogram.count(2L).sum(5000.0)))
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
        .unit("s")
        .points(PointSetExpectation.exists(PointExpectation.histogram.count(1L)))
    )
  }

  test("5b.numericGauge maps to an ObservableGauge and records the value with attributes") {
    // A numericGauge maps to an otel4s ObservableGauge, whose callback only reports while the registration
    // resource is open. Unlike the push instruments, it records nothing after release, so metrics must be
    // collected while the gauge is still alive: collectMetrics runs inside the facilitate `use` scope.
    val metrics = MetricsTestkit.inMemory[IO]().use { testkit =>
      val service =
        TaskGuard[IO]("otel")
          .service("otel")
          .updateConfig(_.withMeterProvider(Resource.pure(testkit.meterProvider)))
      Ref.of[IO, List[MetricData]](Nil).flatMap { collected =>
        service
          .eventStream(agent =>
            agent
              .facilitate("hub")(_.numericGauge("queue_depth", IO.pure(7L)))
              .use(_ => testkit.collectMetrics.flatMap(collected.set)))
          .compile
          .drain >> collected.get
      }
    }.unsafeRunSync()

    assertMetrics(
      metrics,
      MetricExpectation
        .gauge[Long]("queue_depth")
        .points(PointSetExpectation.exists(
          PointExpectation.numeric(7L).attributesExact(njDomain, njService, njTask)))
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
