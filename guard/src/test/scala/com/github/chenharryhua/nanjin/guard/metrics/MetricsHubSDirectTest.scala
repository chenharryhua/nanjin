package com.github.chenharryhua.nanjin.guard.metrics

import cats.data.Ior
import cats.effect.IO
import cats.effect.std.Dispatcher
import com.codahale.metrics.{MetricRegistry, SlidingWindowReservoir}
import com.github.chenharryhua.nanjin.guard.config.{Domain, Service, Task}
import com.github.chenharryhua.nanjin.guard.metrics.api.{Counter, Histogram, Meter, Timer}
import com.github.chenharryhua.nanjin.guard.metrics.api.gauges.{
  ActiveGauge,
  FrequencyCounter,
  IdleGauge,
  Ratio
}
import io.circe.Json
import org.typelevel.otel4s.metrics.{BucketBoundaries, MeterProvider}
import squants.information.Bytes
import squants.time.Milliseconds
import munit.CatsEffectSuite

import java.time.{Duration as JavaDuration, ZoneId}
import scala.concurrent.duration.DurationInt

/** Direct, service-free tests for the stream adapter and metric API edge paths.
  *
  * Every `MetricsHubS` method is exercised against a real Dropwizard registry and a no-op otel4s meter. Each
  * stream asserts registration while its resource is active; the shared helper then verifies release removed
  * every metric from the registry.
  */
class MetricsHubSDirectTest extends CatsEffectSuite {

  private val SCOPE =
    MetricScope(MetricScope.Label("direct"), Domain("test"), Service("metrics"), Task("hub-s"))

  private def with_hub[A](run: (MetricsHubS[IO], MetricRegistry) => IO[A]): IO[A] =
    Dispatcher.parallel[IO].use { dispatcher =>
      MeterProvider.noop[IO].get("direct-test").flatMap { otel_meter =>
        val registry = new MetricRegistry
        val hub = MetricsHub[IO](SCOPE, registry, dispatcher, ZoneId.of("UTC"), otel_meter)
        run(MetricsHubS(hub), registry).flatTap(_ => IO(assert(registry.getMetrics.isEmpty)))
      }
    }

  test("1.counter, meter, histogram, and timer streams delegate builders and unregister on release") {
    with_hub { (hub, registry) =>
      val counter = hub
        .counter("counter", _.withDescription("count").withAttributes("kind" -> "direct"))
        .evalMap(_.inc(2) >> IO(assertEquals(registry.getCounters.size(), 1)))
        .compile
        .drain

      val meter = hub
        .meter("meter", _.withDescription("rate").withUnit(Bytes).withAttributes("kind" -> "direct"))
        .evalMap(_.mark(3) >> IO(assertEquals(registry.getMeters.size(), 1)))
        .compile
        .drain

      val histogram = hub
        .histogram(
          "histogram",
          _.withReservoir(new SlidingWindowReservoir(16))
            .withDescription("distribution")
            .withUnit(Bytes)
            .withBoundaries(BucketBoundaries(1.0, 5.0))
            .withAttributes("kind" -> "direct")
        )
        .evalMap(_.update(4) >> IO(assertEquals(registry.getHistograms.size(), 1)))
        .compile
        .drain

      val timer = hub
        .timer(
          "timer",
          _.withReservoir(new SlidingWindowReservoir(16))
            .withDescription("latency")
            .withTimeUnit(Milliseconds)
            .withBoundaries(BucketBoundaries(1.0, 5.0))
            .withAttributes("kind" -> "direct")
        )
        .evalMap { timer =>
          timer.elapsed(2.millis) >>
            timer.elapsed(JavaDuration.ofMillis(3)) >>
            timer.timing(IO.pure(7)).flatMap(result => IO(assertEquals(result, 7))) >>
            IO(assertEquals(registry.getTimers.size(), 1))
        }
        .compile
        .drain

      counter >> meter >> histogram >> timer
    }
  }

  test("2.gauge, numericGauge, and healthCheck streams register while active and release cleanly") {
    with_hub { (hub, registry) =>
      val gauge = hub
        .gauge("gauge", _.withTimeout(1.second).withPolicy(None).register(IO.pure(42)))
        .evalMap(_ => IO(assertEquals(registry.getGauges.size(), 1)))
        .compile
        .drain

      val numeric = hub
        .numericGauge(
          "numeric",
          IO.pure(9L),
          _.withTimeout(1.second)
            .withUnit(Bytes)
            .withDescription("queue depth")
            .withAttributes("kind" -> "direct"))
        .evalMap(_ => IO(assertEquals(registry.getGauges.size(), 1)))
        .compile
        .drain

      val health = hub
        .healthCheck("health", _.withTimeout(1.second).withPolicy(_.empty).register(IO.pure(true)))
        .evalMap(_ => IO(assertEquals(registry.getGauges.size(), 1)))
        .compile
        .drain

      gauge >> numeric >> health
    }
  }

  test("3.ratio, idle, active, and frequency streams expose their controls") {
    with_hub { (hub, registry) =>
      val ratio = hub
        .ratio(
          "ratio",
          _.withTranslator(cats.data.Reader((ior: Ior[Long, Long]) => Json.fromString(ior.toString))))
        .evalMap(_.run(Ior.both(1L, 2L)) >> IO(assertEquals(registry.getGauges.size(), 1)))
        .compile
        .drain

      val idle = hub
        .idleGauge("idle", _.enable(true))
        .evalMap(_.wakeUp >> IO(assertEquals(registry.getGauges.size(), 1)))
        .compile
        .drain

      val active = hub
        .activeGauge("active", _.enable(true))
        .evalMap(_.deactivate >> IO(assertEquals(registry.getGauges.size(), 1)))
        .compile
        .drain

      val frequency = hub
        .frequencyCounter("frequency", _.enable(true))
        .evalMap(counter =>
          counter.inc("new") >> counter.inc("new", 2L) >> IO(assertEquals(registry.getGauges.size(), 1)))
        .compile
        .drain

      ratio >> idle >> active >> frequency
    }
  }

  test("4.noop metric implementations preserve effects and accept every operation") {
    val failure = new RuntimeException("expected")

    for {
      _ <- Counter.noop[IO].inc(1)
      _ <- Meter.noop[IO].mark(1)
      _ <- Histogram.noop[IO].update(1)
      _ <- Timer.noop[IO].elapsed(1.millis)
      _ <- Timer.noop[IO].elapsed(JavaDuration.ofMillis(1))
      result <- Timer.noop[IO].timing(IO.pure(7))
      failed <- Timer.noop[IO].timing(IO.raiseError[Int](failure)).attempt
      _ <- Ratio.noop[IO].run(Ior.left(1L))
      _ <- Ratio.noop[IO].run(Ior.right(2L))
      _ <- Ratio.noop[IO].run(Ior.both(1L, 2L))
      _ <- IdleGauge.noop[IO].wakeUp
      _ <- ActiveGauge.noop[IO].deactivate
      _ <- FrequencyCounter.noop[IO].inc("tag")
    } yield {
      assertEquals(result, 7)
      assertEquals(failed, Left(failure))
    }
  }

  test("5.disabled stream builders emit no registered metric") {
    with_hub { (hub, registry) =>
      val counter = hub.counter("counter", _.enable(false)).evalMap(_.inc(1)).compile.drain
      val meter = hub.meter("meter", _.enable(false)).evalMap(_.mark(1)).compile.drain
      val histogram = hub.histogram("histogram", _.enable(false)).evalMap(_.update(1)).compile.drain
      val timer = hub.timer("timer", _.enable(false)).evalMap(_.elapsedNano(1)).compile.drain
      val numeric = hub.numericGauge("numeric", IO.pure(1L), _.enable(false)).compile.drain
      val ratio = hub.ratio("ratio", _.enable(false)).evalMap(_.incBoth(1L, 2L)).compile.drain
      val idle = hub.idleGauge("idle", _.enable(false)).evalMap(_.wakeUp).compile.drain
      val active = hub.activeGauge("active", _.enable(false)).evalMap(_.deactivate).compile.drain

      (counter >> meter >> histogram >> timer >> numeric >> ratio >> idle >> active) >>
        IO(assert(registry.getMetrics.isEmpty))
    }
  }

  test("6.default stream overloads acquire every metric with optional builder arguments") {
    with_hub { (hub, _) =>
      hub.counter("counter-default").compile.drain >>
        hub.meter("meter-default").compile.drain >>
        hub.histogram("histogram-default").compile.drain >>
        hub.timer("timer-default").compile.drain >>
        hub.numericGauge("numeric-default", IO.pure(1L)).compile.drain >>
        hub.ratio("ratio-default").compile.drain >>
        hub.idleGauge("idle-default").compile.drain >>
        hub.activeGauge("active-default").compile.drain
    }
  }
}
