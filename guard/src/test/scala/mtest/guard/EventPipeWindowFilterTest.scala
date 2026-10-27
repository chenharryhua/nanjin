package mtest.guard

import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{ServiceIdentity, Timestamp}
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot
import com.github.chenharryhua.nanjin.guard.event.{Event, EventPipe, Took}
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.Snapshot
import munit.FunSuite

import java.time.{Instant, ZoneId}
import java.util.UUID
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** Verification unit tests for `EventPipe.windowFilter` (report item V1).
  *
  * These run the filter predicate directly against hand-built synthetic tick sequences, with no running
  * service and no wall-clock sleeping, so the exact selection is deterministic and auditable. Only the
  * snapshot `index` (the tick) drives the filter decision, so the other `MetricsSnapshot` fields are left
  * null — the same technique `TranslatorMonadTest` uses. Production source is not modified.
  */
class EventPipeWindowFilterTest extends FunSuite {

  private val zone: ZoneId = ZoneId.of("UTC")
  private val sequenceId: UUID = UUID.randomUUID()
  // a fixed launch instant; every tick below shares it as launchTime so the filter's
  // `between(launchTime, conclude)` math is anchored to a known origin.
  private val t0: Instant = Instant.parse("2024-01-01T00:00:00Z")

  /** Build a periodic-snapshot event for a tick whose window is `(commenceOffset, concludeOffset]` seconds
    * after `t0`, all sharing `launchTime == t0`.
    */
  private def periodic(commenceOffsetSec: Long, concludeOffsetSec: Long): Event = {
    val tick = Tick(
      sequenceId = sequenceId,
      launchTime = t0,
      zoneId = zone,
      index = concludeOffsetSec, // index is irrelevant to windowFilter; use a readable value
      commence = t0.plusSeconds(commenceOffsetSec),
      acquires = t0.plusSeconds(commenceOffsetSec),
      conclude = t0.plusSeconds(concludeOffsetSec)
    )
    MetricsSnapshot(
      serviceIdentity = null.asInstanceOf[ServiceIdentity],
      logLink = None,
      index = MetricsSnapshot.Periodic(tick),
      snapshot = null.asInstanceOf[Snapshot],
      took = null.asInstanceOf[Took]
    )
  }

  /** The conclude-offsets (seconds after t0) of the ticks kept by `windowFilter(interval)`, given a tick
    * cadence of `step` seconds up to and including `lastSec`.
    */
  private def keptConcludes(interval: FiniteDuration, step: Long, lastSec: Long): List[Long] = {
    val pipe = EventPipe.windowFilter(interval)
    val events =
      (step to lastSec by step).toList.map(sec => periodic(sec - step, sec))
    events.filter(pipe.filter).collect { case MetricsSnapshot(_, _, MetricsSnapshot.Periodic(tick), _, _) =>
      java.time.Duration.between(t0, tick.conclude).toSeconds
    }
  }

  test("1.adhoc snapshots always pass through windowFilter") {
    val adhoc = MetricsSnapshot(
      serviceIdentity = null.asInstanceOf[ServiceIdentity],
      logLink = None,
      index = MetricsSnapshot.Adhoc(Timestamp(t0.atZone(zone))),
      snapshot = null.asInstanceOf[Snapshot],
      took = null.asInstanceOf[Took]
    )
    assert(EventPipe.windowFilter(10.seconds).filter(adhoc))
  }

  test("2.ticks finer than the interval are thinned to about one per interval") {
    // interval 10s, ticks every 3s out to 30s. Boundaries t0+10, t0+20, t0+30 each fall inside exactly
    // one tick's (commence, conclude] window: conclude 12 covers boundary 10, conclude 21 covers 20, and
    // conclude 30 lands exactly on boundary 30 (inclusive upper bound). Boundary t0+0 is inside no window.
    val kept = keptConcludes(10.seconds, step = 3, lastSec = 30)
    assertEquals(kept, List(12L, 21L, 30L))
  }

  test("3.ticks coarser than the interval still keep one tick per covered boundary") {
    // interval 10s, ticks every 7s out to 35s. Windows: (0,7] (7,14] (14,21] (21,28] (28,35].
    // Expected boundaries: 0,10,20,30 -> fall in windows (-,-], (7,14], (14,21], (28,35].
    // So concludes 14, 21, 35 are kept; 28 maps back to boundary 20 (already matched) and is dropped.
    val kept = keptConcludes(10.seconds, step = 7, lastSec = 35)
    assertEquals(kept, List(14L, 21L, 35L))
  }

  test("4.each kept tick corresponds to a distinct interval boundary (no interval double-counted)") {
    // Across a longer finer-grained run, the number of kept ticks equals the number of whole intervals
    // whose boundary lands within the covered time span — i.e. no interval boundary is counted twice.
    val kept = keptConcludes(5.seconds, step = 1, lastSec = 30)
    // boundaries 5,10,15,20,25,30 -> with 1s ticks each boundary is exactly a tick conclude and is kept
    assertEquals(kept, List(5L, 10L, 15L, 20L, 25L, 30L))
  }

  test("5.a tick exactly on an interval boundary is kept (open-closed upper bound is inclusive)") {
    // single tick window (5,10], interval 10s: n_interval = 10/10 = 1.0, expected = t0+10 == conclude,
    // which isWithinOpenClosed treats as inside. So the tick is kept.
    val evt = periodic(commenceOffsetSec = 5, concludeOffsetSec = 10)
    assert(EventPipe.windowFilter(10.seconds).filter(evt))
  }

  test("6.a tick whose expected boundary precedes its window is dropped") {
    // window (6,9], interval 10s: n_interval = 9/10 = 0.9, floor 0, expected = t0+0, which is not in
    // (6,9]. The tick is dropped -- this is the first sub-interval tick before any boundary is reached.
    val evt = periodic(commenceOffsetSec = 6, concludeOffsetSec = 9)
    assert(!EventPipe.windowFilter(10.seconds).filter(evt))
  }
}
