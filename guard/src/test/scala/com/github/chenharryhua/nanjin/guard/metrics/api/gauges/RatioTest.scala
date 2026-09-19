package com.github.chenharryhua.nanjin.guard.metrics.api.gauges

import cats.data.Ior
import cats.effect.IO
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.retrieve
import munit.CatsEffectSuite

/** Tests for `Ratio`: the pure default `translator` (percentage rendering and its edge cases), the `noop`
  * instance, and the accumulation semantics (`incNumerator`/`incDenominator`/`incBoth`/`run` combine via the
  * `Ior` semigroup) observed end-to-end through a live registered ratio gauge.
  */
class RatioTest extends CatsEffectSuite {

  // ---- translator (pure) ---------------------------------------------------------------------------

  private def render(ior: Ior[Long, Long]): String =
    Ratio.translator.run(ior).asString.getOrElse(fail(s"expected a JSON string for $ior"))

  test("1.translator: numerator-only (Left) is n/a") {
    assert(render(Ior.left(3L)) == "n/a")
  }

  test("2.translator: denominator-only (Right) is 0.0%") {
    assert(render(Ior.right(4L)) == "0.0%")
  }

  test("3.translator: both with zero denominator is n/a") {
    assert(render(Ior.both(3L, 0L)) == "n/a")
    assert(render(Ior.both(0L, 0L)) == "n/a")
  }

  test("4.translator: exact and rounded percentages (HALF_UP, 2 decimals)") {
    assert(render(Ior.both(1L, 1L)) == "100.0%")
    assert(render(Ior.both(1L, 2L)) == "50.0%")
    assert(render(Ior.both(0L, 5L)) == "0.0%")
    assert(render(Ior.both(1L, 3L)) == "33.33%") // 33.333... rounds down
    assert(render(Ior.both(2L, 3L)) == "66.67%") // 66.666... rounds up
  }

  test("5.translator: numerator may exceed denominator (over 100%)") {
    assert(render(Ior.both(5L, 4L)) == "125.0%")
  }

  // ---- noop ----------------------------------------------------------------------------------------

  test("6.noop: every operation is a no-op that succeeds") {
    val r = Ratio.noop[IO]
    r.incNumerator(1) >> r.incDenominator(2) >> r.incBoth(3, 4) >> r.run(Ior.both(5, 6))
  }

  // ---- accumulation via a live gauge ---------------------------------------------------------------

  private val service = TaskGuard[IO]("ratio-test").service("ratio")

  private def liveRatioValue(drive: Ratio[IO] => IO[Unit]): IO[String] =
    service
      .eventStream { agent =>
        agent
          .metricsHubS("ratio")
          .ratio("r")
          .evalMap(r => drive(r) >> agent.adhoc.report.void)
          .compile
          .drain
      }
      .mapFilter(Event.metricsSnapshot.getOption)
      .compile
      .lastOrError
      .map { mr =>
        retrieve
          .gauge[String](mr.snapshot.gauges)
          .values
          .headOption
          .getOrElse(fail("no ratio gauge in snapshot"))
      }

  test("7.live ratio accumulates increments across the three inc methods") {
    // 60/500 then +299/+500 via incBoth => 359/1000 = 35.9%
    liveRatioValue(r => r.incDenominator(500) >> r.incNumerator(60) >> r.incBoth(299, 500)).map { value =>
      assert(value == "35.9%")
    }
  }

  test("8.live ratio: run folds an Ior the same as the direct inc methods") {
    // 1/2 accumulated purely through run(Ior)
    liveRatioValue(r => r.run(Ior.both(1L, 2L))).map { value =>
      assert(value == "50.0%")
    }
  }
}
