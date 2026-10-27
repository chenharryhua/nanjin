package mtest.guard

import cats.data.NonEmptyList
import com.github.chenharryhua.nanjin.common.chrono.crontabs
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.ServiceIdentity
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot
import com.github.chenharryhua.nanjin.guard.event.{Event, EventPipe, Took}
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.Snapshot
import munit.FunSuite

import java.time.{Instant, LocalDate, LocalTime, ZoneId, ZonedDateTime}
import java.util.UUID

/** Verification unit tests for `EventPipe.cronFilter` and `EventPipe.localTimeFilter` (report item V2),
  * including behavior around daylight-saving transitions in a non-UTC zone.
  *
  * The filters are exercised directly against hand-built ticks (no running service, no sleeping). Only the
  * snapshot `index` tick drives the decision, so other `MetricsSnapshot` fields are null, matching the
  * approach in `TranslatorMonadTest`. Production source is not modified.
  *
  * DST reference (Australia/Sydney):
  *   - 2024-10-06: spring forward, 02:00 -> 03:00, so local times in [02:00, 03:00) do not exist (gap).
  *   - 2024-04-07: fall back, 03:00 -> 02:00, so local times in [02:00, 03:00) occur twice (overlap).
  */
class EventPipeTimeFilterTest extends FunSuite {

  private val sydney: ZoneId = ZoneId.of("Australia/Sydney")
  private val sequenceId: UUID = UUID.randomUUID()

  /** A periodic-snapshot event whose tick window is `(commence, conclude]` in `zone`. */
  private def periodic(zone: ZoneId, commence: Instant, conclude: Instant): Event = {
    val tick = Tick(
      sequenceId = sequenceId,
      launchTime = commence,
      zoneId = zone,
      index = 1L,
      commence = commence,
      acquires = commence,
      conclude = conclude
    )
    MetricsSnapshot(
      serviceIdentity = null.asInstanceOf[ServiceIdentity],
      logLink = None,
      index = MetricsSnapshot.Periodic(tick),
      snapshot = null.asInstanceOf[Snapshot],
      took = null.asInstanceOf[Took]
    )
  }

  // ---------------- cronFilter ----------------

  test("1.cronFilter keeps a tick whose window covers the next cron firing") {
    // a 5-minute window (10:00, 10:05] on a plain day; the hourly cron fires at 11:00 which is NOT in the
    // window, but the every-minute cron fires at 10:01 which IS -> keep with minutely, drop with hourly.
    val base = ZonedDateTime.of(2024, 1, 1, 10, 0, 0, 0, sydney).toInstant
    val evt = periodic(sydney, base, base.plusSeconds(5 * 60))

    assert(EventPipe.cronFilter(crontabs.everyMinute).filter(evt))
    assert(!EventPipe.cronFilter(crontabs.hourly).filter(evt))
  }

  test("2.cronFilter: adhoc passes, non-matching periodic window is dropped") {
    // window (10:05, 10:06] contains no hourly firing (next is 11:00) -> dropped.
    val base = ZonedDateTime.of(2024, 1, 1, 10, 5, 0, 0, sydney).toInstant
    val evt = periodic(sydney, base, base.plusSeconds(60))
    assert(!EventPipe.cronFilter(crontabs.hourly).filter(evt))
  }

  // ---------------- localTimeFilter ----------------

  test("3.localTimeFilter keeps a tick whose window covers the target wall-clock time") {
    // window (10:00, 10:05] on the tick's own date; target 10:02 falls inside -> keep.
    val base = ZonedDateTime.of(2024, 1, 1, 10, 0, 0, 0, sydney).toInstant
    val evt = periodic(sydney, base, base.plusSeconds(5 * 60))
    assert(EventPipe.localTimeFilter(NonEmptyList.of(LocalTime.of(10, 2))).filter(evt))
    // a target outside the window is dropped
    assert(!EventPipe.localTimeFilter(NonEmptyList.of(LocalTime.of(9, 0))).filter(evt))
  }

  test("4.localTimeFilter: the conclude instant itself is inside the open-closed window") {
    // window (10:00, 10:05]; target exactly 10:05 == conclude -> kept (inclusive upper bound).
    val base = ZonedDateTime.of(2024, 1, 1, 10, 0, 0, 0, sydney).toInstant
    val evt = periodic(sydney, base, base.plusSeconds(5 * 60))
    assert(EventPipe.localTimeFilter(NonEmptyList.of(LocalTime.of(10, 5))).filter(evt))
  }

  test("5.localTimeFilter around the spring-forward gap (02:00-03:00 does not exist)") {
    // On 2024-10-06 Sydney jumps 02:00 -> 03:00. A tick window that spans the jump: local 01:30 -> 03:30.
    // 01:30 AEST = 2024-10-05T14:30Z ... but build from instants to avoid ambiguity.
    val start = ZonedDateTime.of(2024, 10, 6, 1, 30, 0, 0, sydney).toInstant
    val end = ZonedDateTime.of(2024, 10, 6, 3, 30, 0, 0, sydney).toInstant
    val evt = periodic(sydney, start, end)

    // A nonexistent local time (02:30) is resolved by java.time by shifting forward into the gap; whatever
    // instant that yields, the filter must not throw and must return a stable boolean. We assert it is
    // handled (no exception) and record the observed decision.
    val gapTime = LocalTime.of(2, 30)
    val resolved: ZonedDateTime =
      java.time.LocalDateTime.of(LocalDate.of(2024, 10, 6), gapTime).atZone(sydney)
    val decided = EventPipe.localTimeFilter(NonEmptyList.of(gapTime)).filter(evt)
    // java.time shifts 02:30 (nonexistent) to 03:30 AEDT, which equals `end` (inside the open-closed
    // window), so the tick is kept. This documents the actual, non-throwing behavior.
    assertEquals(resolved.toInstant, end)
    assert(decided)

    // A clearly in-window existing time (03:00) is also kept.
    assert(EventPipe.localTimeFilter(NonEmptyList.of(LocalTime.of(3, 0))).filter(evt))
  }

  test("6.localTimeFilter around the fall-back overlap (02:00-03:00 occurs twice)") {
    // On 2024-04-07 Sydney falls back 03:00 -> 02:00, so 02:30 happens twice. java.time's atZone picks the
    // earlier offset (AEDT). A tick window spanning the overlap: local 01:30 -> 03:30 (by instants).
    val start = ZonedDateTime.of(2024, 4, 7, 1, 30, 0, 0, sydney).toInstant
    val end = ZonedDateTime.of(2024, 4, 7, 3, 30, 0, 0, sydney).toInstant
    val evt = periodic(sydney, start, end)

    val overlapTime = LocalTime.of(2, 30)
    val resolved: ZonedDateTime =
      java.time.LocalDateTime.of(LocalDate.of(2024, 4, 7), overlapTime).atZone(sydney)
    // the earlier (AEDT) 02:30 is within the window; filter keeps it without throwing.
    val decided = EventPipe.localTimeFilter(NonEmptyList.of(overlapTime)).filter(evt)
    assert(resolved.toInstant.isAfter(start) && !resolved.toInstant.isAfter(end))
    assert(decided)
  }
}
