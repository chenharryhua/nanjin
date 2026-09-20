package mtest.guard

import cats.effect.IO
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.retrieve
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import munit.CatsEffectSuite

/** Tests for `MetricsHubS`, the stream-native adapter over `MetricsHub`. Each method wraps the hub's
  * `Resource` in `Stream.resource`, so the behavior to verify is faithful delegation: the same `scope`, a
  * metric that registers and records while its stream runs, and unregistration once the stream terminates
  * (the `Resource` release). Exercised through the real hub via `Agent.metricsHubS`/`facilitateS`.
  */
class MetricsHubSTest extends CatsEffectSuite {

  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("metrics-hub-s").service("metrics-hub-s")

  private def lastSnapshot(
    body: com.github.chenharryhua.nanjin.guard.service.Agent[IO] => IO[Unit]): IO[MetricsSnapshot] =
    service
      .eventStream(body)
      .map(checkJson)
      .mapFilter(Event.metricsSnapshot.getOption)
      .compile
      .lastOrError

  test("1.scope matches the underlying resource-based hub for the same label") {
    // MetricsHubS just forwards hub.scope, so the two interfaces agree for a given label. The Agent only
    // exists inside eventStream, so assert within the body and drain.
    service
      .eventStream { agent =>
        val streamScope = agent.metricsHubS("x").scope
        val hubScope = agent.facilitate("x")(_.scope)
        IO(assert(streamScope == hubScope)).void
      }
      .compile
      .drain
  }

  test("2.counter registered via the stream records into the snapshot") {
    lastSnapshot { agent =>
      agent
        .metricsHubS("counter")
        .counter("counter")
        .evalMap(c => c.inc(10) >> agent.adhoc.report.void)
        .compile
        .drain
    }.map { mr =>
      assert(mr.snapshot.nonEmpty)
      assert(retrieve.counter(mr.snapshot.counters).values.head.value == 10)
      assert(mr.index.isInstanceOf[MetricsSnapshot.Adhoc])
    }
  }

  test("3.meter registered via the stream records into the snapshot") {
    lastSnapshot { agent =>
      agent
        .metricsHubS("meter")
        .meter("meter")
        .evalMap(m => m.mark(10) >> m.mark(20) >> agent.adhoc.report.void)
        .compile
        .drain
    }.map { mr =>
      assert(mr.snapshot.nonEmpty)
      assert(retrieve.meter(mr.snapshot.meters).values.head.aggregate == 30)
    }
  }

  test("4.builder customization flows through (disabled counter is not registered)") {
    lastSnapshot { agent =>
      agent
        .metricsHubS("counter")
        .counter("counter", _.enable(false))
        .evalMap(c => c.inc(10) >> agent.adhoc.report.void)
        .compile
        .drain
    }.map { mr =>
      // a disabled instrument records nothing, exactly as through MetricsHub.counter
      assert(retrieve.counter(mr.snapshot.counters).values.isEmpty)
    }
  }

  test("5.metric is unregistered once its stream terminates") {
    // register + record + report INSIDE the stream, then take a second report AFTER the metric stream
    // has terminated; the counter should be gone from the later snapshot (Resource released on stream end).
    service
      .eventStream { agent =>
        agent
          .metricsHubS("counter")
          .counter("counter")
          .evalMap(c => c.inc(10))
          .compile
          .drain >> agent.adhoc.report.void
      }
      .map(checkJson)
      .mapFilter(Event.metricsSnapshot.getOption)
      .compile
      .toList
      .map { snapshots =>
        // the report taken after the counter stream ended sees no counter
        assert(snapshots.forall(mr => retrieve.counter(mr.snapshot.counters).values.isEmpty))
      }
  }

  test("6.facilitateS exposes the same stream interface") {
    lastSnapshot { agent =>
      agent.facilitateS("counter") { hub =>
        hub.counter("counter").evalMap(c => c.inc(7) >> agent.adhoc.report.void).compile.drain
      }
    }.map { mr =>
      assert(retrieve.counter(mr.snapshot.counters).values.head.value == 7)
    }
  }
}
