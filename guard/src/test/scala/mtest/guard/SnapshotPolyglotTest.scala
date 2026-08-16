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
          counter.inc(42) >>
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
    assert(yaml.contains("aggregate:"))
    assert(yaml.contains("updates:"))
    assert(yaml.contains("invocations:"))

    val slackYaml = polyglot.counterYaml.get
    assert(slackYaml.contains("requests:"))
  }
}
