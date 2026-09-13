package com.github.chenharryhua.nanjin.guard.batch

import cats.effect.IO
import cats.effect.std.Dispatcher
import cats.effect.unsafe.implicits.global
import com.codahale.metrics.{Gauge as CodahaleGauge, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{Domain, Service, Task}
import com.github.chenharryhua.nanjin.guard.metrics.{MetricScope, MetricsHub}
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.otel4s.metrics.MeterProvider

import java.time.ZoneId
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

/** Direct tests for the `BatchPanel` object, the metrics-panel machinery behind `Batch`.
  *
  * Lives in package `com.github.chenharryhua.nanjin.guard.batch` (not `mtest`) so it can reach the
  * package-private `BatchPanel` and the `Job`/`JobRecord`/`BatchMode`/`BatchKind` data model.
  *
  * `MetricsHub` is sealed, so there is no fake — the tests build a real hub over a `MetricRegistry` they own
  * and read each gauge's rendered `Json` straight off the Dropwizard registry. That directly observes the two
  * pieces `BatchPanel` is responsible for: the completion ratio string produced by its custom `translator`,
  * and the "Completed jobs" object produced by `jobRecordsToJson`.
  */
class PanelTest extends AnyFunSuite {

  private val scope =
    MetricScope(MetricScope.Label("batch"), Domain("test"), Service("test-service"), Task("task"))
  private val batchId: BatchId = BatchId(1L)

  private def job(name: String, index: Int, kind: Option[BatchKind]): Job =
    Job(
      name = name,
      index = index,
      scope = scope,
      mode = BatchMode.Sequential,
      kind = kind,
      batchId = batchId)

  private def record(name: String, index: Int, succeeded: Boolean): JobRecord =
    JobRecord(
      job(name, index, Some(BatchKind.Quasi)),
      start = 0.seconds,
      end = 5.seconds,
      succeeded = succeeded)

  /** Run `f` against a real hub built over a fresh registry, then hand back every gauge's rendered Json so a
    * test can assert on the panel's output. The `Dispatcher` is required by the gauge machinery, which runs
    * the registered effect on scrape.
    */
  private def withHub[A](f: (MetricsHub[IO], () => List[Json]) => IO[A]): A =
    Dispatcher
      .parallel[IO]
      .use { dispatcher =>
        val registry = new MetricRegistry
        val hub = MetricsHub[IO](scope, registry, dispatcher, ZoneId.systemDefault(), MeterProvider.noop[IO])
        val readGauges: () => List[Json] =
          () => registry.getGauges.asScala.values.toList.map(_.asInstanceOf[CodahaleGauge[Json]].getValue)
        f(hub, readGauges)
      }
      .unsafeRunSync()

  // ---- BatchPanel.apply: completion ratio ----------------------------------------------------------

  test("1.BatchPanel seeds the ratio denominator with the job count") {
    val gauges = withHub { (hub, readGauges) =>
      BatchPanel(hub, size = 3, BatchKind.Value, BatchMode.Sequential).use { _ =>
        IO(readGauges())
      }
    }
    // no jobs completed yet: numerator 0 of denominator 3 -> "0.0% (0/3)"
    assert(gauges.flatMap(_.asString).exists(_ == "0.0% (0/3)"))
  }

  test("2.BatchPanel: update bumps the numerator and renders a percentage") {
    val gauges = withHub { (hub, readGauges) =>
      BatchPanel(hub, size = 2, BatchKind.Value, BatchMode.Sequential).use { bm =>
        bm.update.run(record("a", 1, succeeded = true)) *> IO(readGauges())
      }
    }
    // one of two done -> the panel's translator renders "50.0% (1/2)"
    assert(gauges.flatMap(_.asString).exists(_ == "50.0% (1/2)"))
  }

  // ---- BatchPanel.apply: completed-jobs gauge ------------------------------------------------------

  test("3.BatchPanel: Completed jobs gauge is Json.Null before any job completes") {
    val gauges = withHub { (hub, readGauges) =>
      BatchPanel(hub, size = 1, BatchKind.Quasi, BatchMode.Sequential).use { _ =>
        IO(readGauges())
      }
    }
    // an empty progress list renders as Json.Null (jobRecordsToJson)
    assert(gauges.contains(Json.Null))
  }

  test("4.BatchPanel: completed jobs render keyed by displayName, sorted by index, failed suffixed") {
    val completed = withHub { (hub, readGauges) =>
      BatchPanel(hub, size = 2, BatchKind.Quasi, BatchMode.Sequential).use { bm =>
        // apply out of index order to prove the render sorts by index
        bm.update.run(record("beta", 2, succeeded = false)) *>
          bm.update.run(record("alpha", 1, succeeded = true)) *>
          IO(readGauges())
      }
    }
    // find the object-shaped gauge (the "Completed jobs" render)
    val obj = completed.flatMap(_.asObject).headOption.getOrElse(fail("no Completed jobs object gauge"))
    val keys = obj.keys.toList
    assert(keys == List("job-1 alpha", "job-2 beta")) // sorted by index, keyed by displayName
    assert(obj("job-1 alpha").flatMap(_.asString).exists(!_.contains("failed")))
    assert(obj("job-2 beta").flatMap(_.asString).exists(_.contains("(failed)")))
  }

  // ---- BatchPanel.monadic --------------------------------------------------------------------------

  test("5.BatchPanel.monadic has no ratio gauge; progress starts null then renders completed jobs") {
    val (before, after) = withHub { (hub, readGauges) =>
      BatchPanel.monadic(hub).use { bm =>
        for {
          b <- IO(readGauges())
          _ <- bm.update.run(record("only", 1, succeeded = true))
          a <- IO(readGauges())
        } yield (b, a)
      }
    }
    // before any completion: no percentage/ratio string is present (monadic panel registers no ratio)
    assert(!before.flatMap(_.asString).exists(_.contains("/")))
    // after one completion the progress gauge renders the single job keyed by displayName
    val obj = after.flatMap(_.asObject).headOption.getOrElse(fail("no progress object gauge"))
    assert(obj.keys.toList == List("job-1 only"))
  }
}
