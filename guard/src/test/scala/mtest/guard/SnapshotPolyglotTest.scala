package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.{retrieve, SnapshotPolyglot}
import org.scalatest.funsuite.AnyFunSuite
import squants.information.Bytes

class SnapshotPolyglotTest extends AnyFunSuite {
  private val service = TaskGuard[IO]("snapshot").service("snapshot")

  test("renders the full snapshot across JSON and YAML formats") {
    val snapshot = service.eventStream { agent =>
      agent
        .facilitate("snapshot") { fac =>
          for {
            counter <- fac.counter("requests")
            meter <- fac.meter("throughput")
            histogram <- fac.histogram("samples", _.withUnit(Bytes))
            timer <- fac.timer("latency")
          } yield (counter, meter, histogram, timer)
        }
        .use { case (counter, meter, histogram, timer) =>
          counter.inc(4200) >>
            meter.mark(3) >>
            histogram.update(5) >>
            timer.elapsedNano(4L) >>
            agent.adhoc.report.void
        }
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()

    assert(snapshot.snapshot.nonEmpty)
    assert(retrieve.counter(snapshot.snapshot.counters).values.nonEmpty)
    assert(retrieve.meter(snapshot.snapshot.meters).values.nonEmpty)
    assert(retrieve.histogram(snapshot.snapshot.histograms).values.nonEmpty)
    assert(retrieve.timer(snapshot.snapshot.timers).values.nonEmpty)

    val polyglot = new SnapshotPolyglot(snapshot.snapshot)

    val prettyJson = polyglot.toPrettyJson.noSpaces
    assert(prettyJson.contains("requests"))
    assert(prettyJson.contains("throughput"))
    assert(prettyJson.contains("samples"))
    assert(prettyJson.contains("latency"))

    val vanillaJson = polyglot.toVanillaJson.noSpaces
    assert(vanillaJson.contains("requests"))

    val yaml = polyglot.toYaml
    assert(yaml.contains("requests:"))
    assert(yaml.contains("4,200"))
    assert(yaml.contains("aggregate:"))
    assert(yaml.contains("updates:"))
    assert(yaml.contains("invocations:"))

  }

  test("domains render in a stable, deterministic order across formats and repeated renders") {
    // Two distinct domains; metrics are registered in a fixed order so their ages are ordered.
    // Register the alphabetically-LATER domain ("zulu") FIRST, so registration/age order contradicts name
    // order. This lets the assertions distinguish age-primary ordering from a mere alphabetical fallback.
    val snapshot = service.eventStream { agent =>
      val zulu = agent.withDomain("zulu")
      val alpha = agent.withDomain("alpha")
      (
        zulu.facilitate("m")(_.counter("z-counter")),
        alpha.facilitate("m")(_.counter("a-counter"))
      ).tupled.use { case (z, a) =>
        z.inc(1) >> a.inc(2) >> agent.adhoc.report.void
      }
    }.map(checkJson).mapFilter(Event.metricsSnapshot.getOption).compile.lastOrError.unsafeRunSync()

    val polyglot = new SnapshotPolyglot(snapshot.snapshot)

    // Both domains appear.
    val yaml = polyglot.toYaml
    assert(yaml.contains("[alpha]:"))
    assert(yaml.contains("[zulu]:"))

    // Rendering is a pure function of the snapshot: repeated renders are byte-identical. This guards the
    // domain ordering, which previously relied on nondeterministic groupBy(...).toList order and is now
    // pinned by an explicit (age, name) sort key.
    assert(polyglot.toYaml == yaml)
    val json = polyglot.toPrettyJson.noSpaces
    assert(polyglot.toPrettyJson.noSpaces == json)
    assert(new SnapshotPolyglot(snapshot.snapshot).toYaml == yaml)
    assert(new SnapshotPolyglot(snapshot.snapshot).toPrettyJson.noSpaces == json)

    // zulu registered first, so ordering is by age (registration order), NOT alphabetical: zulu precedes
    // alpha in both renderers.
    assert(yaml.indexOf("[zulu]:") < yaml.indexOf("[alpha]:"))
    assert(json.indexOf("zulu") < json.indexOf("alpha"))
  }
}
