package com.github.chenharryhua.nanjin.guard.event

import cats.data.NonEmptyList
import cats.syntax.order.given
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot.{Adhoc, Periodic}
import cron4s.lib.javatime.javaTemporalInstance
import cron4s.{toDateTimeCronOps, CronExpr}

import java.time.{Duration, Instant, LocalDate, LocalDateTime, LocalTime, ZoneId}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

/** Predicate-based event filter that can be composed and applied to event streams.
  *
  * Each pipe decides, per `Event`, whether the event is kept. Pipes are pure and stateless, so the same
  * instance can be reused across streams and composed freely.
  *
  * Most built-in filters in the companion follow a shared convention: non-`MetricsSnapshot` events pass
  * through unchanged, and among snapshots only `Periodic` ones are subject to filtering while `Adhoc`
  * snapshots pass through. That covers `cronFilter`, `localTimeFilter`, `indexFilter` and `windowFilter`. Two
  * filters sit outside it: `logLevel` targets `ReportedEvent` instead, and `noAdhoc` inverts the snapshot
  * clause outright — it is the one filter that drops `Adhoc` snapshots and passes every `Periodic` one.
  */
trait EventPipe { self =>

  /** Decide the fate of a single event: `Some(event)` to keep it, `None` to drop it. The event may be
    * returned unchanged; pipes filter rather than transform.
    */
  def apply(event: Event): Option[Event]

  /** Boolean view of `apply` for use with `Stream.filter` and similar predicate-based APIs: `true` when the
    * event is kept.
    */
  final def filter(event: Event): Boolean =
    apply(event).isDefined

  /** Compose two pipes into one that keeps an event only if both keep it (logical AND). Short-circuits: if
    * `self` drops the event, `other` is not consulted. `identity` is the unit of this operation.
    */
  final def &&(other: EventPipe): EventPipe =
    new EventPipe {
      def apply(event: Event): Option[Event] =
        self(event).flatMap(other(_))
    }
}

object EventPipe {

  /** Build a pipe from an arbitrary decision function.
    *
    * Only whether `f` returns `Some` is ever observed: every consumer goes through `EventPipe.filter` (which
    * is `apply(event).isDefined`), including `Translator.filter(pipe)` and the `Stream.filter` call sites. So
    * the event inside the `Some` is '''discarded''' — a function of the form `e => Some(transform(e))`
    * type-checks and reads as though it rewrites the event, but the transformation is silently dropped
    * wherever the pipe is used. Return the event unchanged, or `None`.
    */
  def apply(f: Event => Option[Event]): EventPipe =
    new EventPipe { override def apply(e: Event): Option[Event] = f(e) }

  /** Pipe that keeps every event. Acts as the unit of `EventPipe.&&`, so `identity && p` behaves like `p`.
    */
  val identity: EventPipe = new EventPipe {
    override def apply(event: Event): Option[Event] = Some(event)
  }

  /** Drop `ReportedEvent`s whose level is below `threshold`; all other events pass through.
    *
    * @param f
    *   selects the threshold from the `LogLevel` companion, e.g. `_.Warn`. Events at or above the threshold
    *   are kept.
    */
  def logLevel(f: LogLevel.type => LogLevel): EventPipe =
    new EventPipe {
      private val threshold: LogLevel = f(LogLevel)
      override def apply(event: Event): Option[Event] =
        event match {
          case evt @ Event.ReportedEvent(_, _, _, _, _, report) =>
            if report.level >= threshold then Some(evt) else None
          case other => Some(other)
        }
    }

  /** Drop `Adhoc` metrics snapshots (those triggered by an on-demand report) while keeping `Periodic` ones;
    * non-snapshot events pass through unchanged.
    */
  def noAdhoc: EventPipe =
    new EventPipe {
      override def apply(event: Event): Option[Event] = event match {
        case Event.MetricsSnapshot(_, _, index, _, _) =>
          index match {
            case Adhoc(_)    => None
            case Periodic(_) => Some(event)
          }
        case others => Some(others)
      }
    }

  /** Keep a periodic `MetricsSnapshot` only when its tick falls in the slot opened by `cronExpr`; `Adhoc`
    * snapshots and non-snapshot events always pass through.
    *
    * A tick is kept when the cron's next firing after the tick's commence time lands within the tick's
    * open-closed window, i.e. the tick is the one that covers a scheduled cron instant.
    *
    * @param cronExpr
    *   the cron schedule defining which periodic snapshots to keep.
    */
  def cronFilter(cronExpr: CronExpr): EventPipe =
    new EventPipe {
      override def apply(event: Event): Option[Event] =
        event match {
          case MetricsSnapshot(_, _, index, _, _) =>
            index match {
              case Adhoc(_)       => Some(event)
              case Periodic(tick) =>
                val inSlot =
                  cronExpr.next(tick.zoned(_.commence))
                    .exists(zdt => tick.isWithinOpenClosed(zdt.toInstant))
                if (inSlot) Some(event) else None
            }
          case others => Some(others)
        }
    }

  /** Keep a periodic `MetricsSnapshot` only when its tick's window covers a real occurrence of one of the
    * given wall-clock times, read in the tick's zone; `Adhoc` snapshots and non-snapshot events always pass
    * through.
    *
    * Like `cronFilter`, this asks whether a scheduled instant falls inside the tick's open-closed window,
    * rather than deriving a single instant from one date and testing that. The distinction matters in three
    * cases, each of which the date-derived form got wrong:
    *   - '''Across midnight.''' A window spanning midnight touches two dates. Keying off the conclude date
    *     alone moved a target just before midnight onto the following day, where no tick could cover it.
    *   - '''Windows of a day or more.''' A coarse report policy gives a window spanning whole days; only the
    *     dates the window actually touches are considered, so targets on an earlier date are no longer
    *     missed.
    *   - '''Daylight-saving transitions.''' See `occurrences` for why `getValidOffsets` replaces
    *     `LocalDateTime.atZone` here.
    *
    * The date scan is proportional to the window's length in days, which is one or two for any ordinary
    * report policy.
    *
    * @param localTimes
    *   the times of day to keep. A tick is kept if any real occurrence of any of them falls within the tick's
    *   open-closed window.
    */
  def localTimeFilter(localTimes: NonEmptyList[LocalTime]): EventPipe =
    new EventPipe {

      /** Every '''real''' occurrence of `lt` on `date` in `zone`, which is none, one, or two instants.
        *
        * `ZoneRules.getValidOffsets` is used rather than `LocalDateTime.atZone` because the two disagree
        * exactly where it matters:
        *   - in a daylight-saving '''overlap''' the wall-clock time happens twice, and `atZone` returns only
        *     the earlier offset, so the later occurrence could never be matched. Both are returned here.
        *   - in a spring-forward '''gap''' the wall-clock time does not happen at all, and `atZone` shifts it
        *     forward out of the gap, inventing an occurrence at a different wall-clock time (a filter set for
        *     02:30 silently fired at 03:30). Here the result is empty, so a target that does not exist on
        *     that date does not fire.
        */
      private def occurrences(zone: ZoneId, date: LocalDate, lt: LocalTime): List[Instant] = {
        val ldt: LocalDateTime = LocalDateTime.of(date, lt)
        zone.getRules.getValidOffsets(ldt).asScala.toList.map(ldt.toInstant)
      }

      /** Every date the tick's window touches, in the tick's zone, from the commence date through the
        * conclude date inclusive.
        */
      private def dates(tick: Tick): List[LocalDate] = {
        val last: LocalDate = tick.local(_.conclude).toLocalDate
        Iterator
          .iterate(tick.local(_.commence).toLocalDate)(_.plusDays(1))
          .takeWhile(!_.isAfter(last))
          .toList
      }

      override def apply(event: Event): Option[Event] =
        event match {
          case MetricsSnapshot(_, _, index, _, _) =>
            val isKeep = index match {
              case Adhoc(_)       => true
              case Periodic(tick) =>
                val days = dates(tick)
                localTimes.exists(lt =>
                  days.exists(date => occurrences(tick.zoneId, date, lt).exists(tick.isWithinOpenClosed)))
            }
            if isKeep then Some(event) else None
          case others => Some(others)
        }
    }

  /** Keep every `divisor`-th periodic `MetricsSnapshot` (by tick index); other events pass through unchanged.
    *
    * @param divisor
    *   must be `>= 1`. Validated eagerly so a misconfiguration fails at construction with a clear message,
    *   rather than throwing `ArithmeticException` (`/ by zero`) or silently misfiltering (negative values) on
    *   the first periodic snapshot.
    */
  def indexFilter(divisor: Int): EventPipe = {
    require(divisor >= 1, s"indexFilter divisor must be >= 1, but was $divisor")
    new EventPipe {
      override def apply(event: Event): Option[Event] =
        event match {
          case MetricsSnapshot(_, _, index, _, _) =>
            index match {
              case Adhoc(_)       => Some(event)
              case Periodic(tick) => if ((tick.index % divisor) === 0) Some(event) else None
            }
          case other => Some(other)
        }
    }
  }

  /** Thin periodic `MetricsSnapshot`s to about one per `interval` elapsed since the tick sequence launched;
    * `Adhoc` snapshots and non-snapshot events always pass through.
    *
    * For each tick, the number of whole `interval`s since `launchTime` picks an expected boundary instant;
    * the tick is kept when it covers that boundary. Unlike `indexFilter` (which counts snapshots), this
    * filters by elapsed wall-clock time, so it does not have to be retuned when the snapshot frequency
    * changes.
    *
    * ===What the spacing actually is===
    * The achieved spacing is `max(interval, report period)`, not `interval`, and the filter can only ever
    * thin the stream it is given:
    *   - an `interval` no larger than a tick's window puts a boundary inside '''every''' window, so nothing
    *     is dropped at all — `windowFilter(500.millis)` against a per-second report policy keeps every
    *     snapshot;
    *   - when several boundaries fall inside one window, that tick is still kept exactly once.
    *
    * So "about one per `interval`" holds in the intended regime, where `interval` comfortably exceeds the
    * report cadence.
    *
    * @param interval
    *   the target spacing between kept snapshots. Must be positive. Validated eagerly, as `indexFilter`
    *   validates its divisor, so a misconfiguration fails at construction rather than silently dropping every
    *   periodic snapshot: a zero interval makes the elapsed-interval count `Infinity` (and so `Long.MaxValue`
    *   once truncated) while a negative one makes it negative, each placing the expected boundary where no
    *   tick can cover it.
    */
  def windowFilter(interval: FiniteDuration): EventPipe = {
    require(interval.toNanos > 0, s"windowFilter interval must be positive, but was $interval")
    new EventPipe {

      override def apply(event: Event): Option[Event] =
        event match {
          case MetricsSnapshot(_, _, index, _, _) =>
            index match {
              case Adhoc(_)       => Some(event)
              case Periodic(tick) =>
                val n_interval: Double =
                  Duration.between(tick.launchTime, tick.conclude).toScala / interval
                val expected: Instant =
                  tick.launchTime.plus((n_interval.toLong * interval).toJava)
                if (tick.isWithinOpenClosed(expected)) Some(event) else None
            }
          case other => Some(other)
        }
    }
  }

}
