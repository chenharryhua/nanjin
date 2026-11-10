package mtest.guard

import cats.data.NonEmptyList
import com.github.chenharryhua.nanjin.common.chrono.crontabs
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{ServiceIdentity, Timestamp}
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

  /** An adhoc-snapshot event timestamped at `scrapeTime` in `zone`; other fields are null, like `periodic`.
    */
  private def adhoc(zone: ZoneId, scrapeTime: Instant): Event =
    MetricsSnapshot(
      serviceIdentity = null.asInstanceOf[ServiceIdentity],
      logLink = None,
      index = MetricsSnapshot.Adhoc(Timestamp(scrapeTime.atZone(zone))),
      snapshot = null.asInstanceOf[Snapshot],
      took = null.asInstanceOf[Took]
    )

  /** The decision the previous, date-derived formulation of `localTimeFilter` made: one candidate instant per
    * target, built from the '''conclude date alone''' and resolved with `LocalDateTime.atZone`.
    *
    * Kept here, rather than verified by temporarily reverting the source, so each test below can state
    * exactly what changed and why. Where this disagrees with the filter, the disagreement is the fix.
    */
  private def legacyDecision(event: Event, localTimes: NonEmptyList[LocalTime]): Boolean =
    event match {
      case MetricsSnapshot(_, _, MetricsSnapshot.Periodic(tick), _, _) =>
        localTimes.exists { lt =>
          val zdt =
            java.time.LocalDateTime.of(tick.local(_.conclude).toLocalDate, lt).atZone(tick.zoneId)
          tick.isWithinOpenClosed(zdt.toInstant)
        }
      case _ => true
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

  test("5.localTimeFilter does not fire for a wall-clock time inside the spring-forward gap") {
    // On 2024-10-06 Sydney jumps 02:00 AEST -> 03:00 AEDT, so 02:30 does not occur at all that day.
    // Window local 01:30 AEST -> 03:30 AEDT, i.e. (2024-10-05T15:30Z, 2024-10-05T16:30Z].
    val start = ZonedDateTime.of(2024, 10, 6, 1, 30, 0, 0, sydney).toInstant
    val end = ZonedDateTime.of(2024, 10, 6, 3, 30, 0, 0, sydney).toInstant
    val evt = periodic(sydney, start, end)

    val gapTime = LocalTime.of(2, 30)
    // java.time's atZone invents an occurrence by shifting the nonexistent time forward out of the gap, to
    // 03:30 AEDT, which here is exactly `end` and so used to be reported as covered. getValidOffsets
    // reports the truth: the time has no occurrence on this date, so the filter does not fire.
    assertEquals(java.time.LocalDateTime.of(LocalDate.of(2024, 10, 6), gapTime).atZone(sydney).toInstant, end)
    assert(
      sydney.getRules.getValidOffsets(java.time.LocalDateTime.of(LocalDate.of(2024, 10, 6), gapTime))
        .isEmpty)

    assert(legacyDecision(evt, NonEmptyList.of(gapTime)), "the old formulation fired on the shifted instant")
    assert(!EventPipe.localTimeFilter(NonEmptyList.of(gapTime)).filter(evt))

    // a time that does exist, and falls in the window, still fires
    assert(EventPipe.localTimeFilter(NonEmptyList.of(LocalTime.of(3, 0))).filter(evt))
  }

  test("6.localTimeFilter reaches the later occurrence in the fall-back overlap") {
    // On 2024-04-07 Sydney falls back 03:00 AEDT -> 02:00 AEST, so 02:30 occurs twice:
    //   02:30 AEDT (UTC+11) = 2024-04-06T15:30Z, then 02:30 AEST (UTC+10) = 2024-04-06T16:30Z.
    val overlapTime = LocalTime.of(2, 30)
    val earlier = Instant.parse("2024-04-06T15:30:00Z")
    val later = Instant.parse("2024-04-06T16:30:00Z")

    // atZone returns only the earlier one, which is why the later occurrence used to be unreachable
    assertEquals(
      java.time.LocalDateTime.of(LocalDate.of(2024, 4, 7), overlapTime).atZone(sydney).toInstant,
      earlier)

    // a window containing ONLY the later occurrence: (16:00Z, 17:00Z]
    val lateWindow =
      periodic(sydney, Instant.parse("2024-04-06T16:00:00Z"), Instant.parse("2024-04-06T17:00:00Z"))
    assert(
      !legacyDecision(lateWindow, NonEmptyList.of(overlapTime)),
      "the old formulation saw only the earlier occurrence, which is outside this window")
    assert(EventPipe.localTimeFilter(NonEmptyList.of(overlapTime)).filter(lateWindow))

    // and one containing only the earlier occurrence still fires too
    val earlyWindow =
      periodic(sydney, Instant.parse("2024-04-06T15:00:00Z"), Instant.parse("2024-04-06T16:00:00Z"))
    assert(EventPipe.localTimeFilter(NonEmptyList.of(overlapTime)).filter(earlyWindow))

    assert(earlier.isBefore(later))
  }

  test("8.localTimeFilter covers a target before midnight when the window spans midnight") {
    // Window local 2024-01-01 23:59 -> 2024-01-02 00:01. Keying off the conclude date alone put the target
    // on 2024-01-02, where no tick could cover it.
    val start = ZonedDateTime.of(2024, 1, 1, 23, 59, 0, 0, sydney).toInstant
    val end = ZonedDateTime.of(2024, 1, 2, 0, 1, 0, 0, sydney).toInstant
    val evt = periodic(sydney, start, end)

    assert(
      !legacyDecision(evt, NonEmptyList.of(LocalTime.of(23, 59, 30))),
      "the old formulation moved the pre-midnight target onto the conclude date")
    assert(EventPipe.localTimeFilter(NonEmptyList.of(LocalTime.of(23, 59, 30))).filter(evt))
    // the target on the later date is covered as well
    assert(EventPipe.localTimeFilter(NonEmptyList.of(LocalTime.of(0, 0, 30))).filter(evt))
    // a target outside the window is still dropped
    assert(!EventPipe.localTimeFilter(NonEmptyList.of(LocalTime.of(12, 0))).filter(evt))
  }

  test("9.localTimeFilter covers every date a multi-day window touches") {
    // A coarse report policy gives a window spanning whole days. Only the conclude date was considered, so
    // a target that occurred on an earlier date inside the window was missed entirely.
    val start = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, sydney).toInstant
    val end = ZonedDateTime.of(2024, 1, 3, 0, 0, 0, 0, sydney).toInstant
    val evt = periodic(sydney, start, end)

    // 12:00 occurs on 2024-01-01 and 2024-01-02, both inside the window; 2024-01-03T12:00 is past conclude
    assert(
      !legacyDecision(evt, NonEmptyList.of(LocalTime.of(12, 0))),
      "the old formulation only tried the conclude date, whose 12:00 is past conclude")
    assert(EventPipe.localTimeFilter(NonEmptyList.of(LocalTime.of(12, 0))).filter(evt))
    // the commence instant itself is excluded by the open-closed window, so 00:00 is covered only via the
    // interior dates
    assert(EventPipe.localTimeFilter(NonEmptyList.of(LocalTime.of(0, 0))).filter(evt))
  }

  // ---------------- Adhoc snapshots always pass ----------------

  test("7.cronFilter, localTimeFilter, and indexFilter all pass an Adhoc snapshot through unchanged") {
    val base = ZonedDateTime.of(2024, 1, 1, 10, 0, 0, 0, sydney).toInstant
    val evt = adhoc(sydney, base)

    // These schedules/divisors would reject a matching periodic tick, but an Adhoc snapshot bypasses the
    // periodic decision entirely and is always kept.
    assert(EventPipe.cronFilter(crontabs.hourly).filter(evt))
    assert(EventPipe.localTimeFilter(NonEmptyList.of(LocalTime.of(0, 0))).filter(evt))
    assert(EventPipe.indexFilter(3).filter(evt))
  }
}
