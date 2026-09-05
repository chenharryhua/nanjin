package com.github.chenharryhua.nanjin.guard.event

import cats.data.NonEmptyList
import cats.syntax.order.given
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot.Index.{Adhoc, Periodic}
import cron4s.lib.javatime.javaTemporalInstance
import cron4s.{toDateTimeCronOps, CronExpr}

import java.time.{Duration, Instant, LocalDateTime, LocalTime}
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

/** Predicate-based event filter that can be composed and applied to event streams.
  *
  * Each pipe decides, per `Event`, whether the event is kept. Pipes are pure and stateless, so the same
  * instance can be reused across streams and composed freely. The built-in filters in the companion follow a
  * shared convention: non-`MetricsSnapshot` events pass through unchanged, and among snapshots only
  * `Periodic` ones are subject to filtering while `Adhoc` snapshots pass through. `logLevel` is the
  * exception, it targets `ReportedEvent` instead.
  */
trait EventPipe { self =>

  /** Decide the fate of a single event: `Some(event)` to keep it, `None` to drop it. The event may be
    * returned unchanged; pipes filter rather than transform.
    */
  def apply(event: Event): Option[Event]

  /** Boolean view of [[apply]] for use with `Stream.filter` and similar predicate-based APIs: `true` when the
    * event is kept.
    */
  final def filter(event: Event): Boolean =
    apply(event).isDefined

  /** Compose two pipes into one that keeps an event only if both keep it (logical AND). Short-circuits: if
    * `self` drops the event, `other` is not consulted. [[identity]] is the unit of this operation.
    */
  final def &&(other: EventPipe): EventPipe =
    new EventPipe {
      def apply(event: Event): Option[Event] =
        self(event).flatMap(other(_))
    }
}

object EventPipe {

  /** Build a pipe from an arbitrary decision function. */
  def apply(f: Event => Option[Event]): EventPipe =
    new EventPipe { override def apply(e: Event): Option[Event] = f(e) }

  /** Pipe that keeps every event. Acts as the unit of [[EventPipe.&&]], so `identity && p` behaves like `p`.
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
          case evt @ Event.ReportedEvent(_, _, _, _, level, _, _) =>
            if level >= threshold then Some(evt) else None
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

  /** Keep a periodic `MetricsSnapshot` only when its tick covers one of the given wall-clock times on the
    * tick's own date (in the tick's zone); `Adhoc` snapshots and non-snapshot events always pass through.
    *
    * @param localTimes
    *   the times of day to keep. A tick is kept if any of them falls within the tick's open-closed window.
    */
  def localTimeFilter(localTimes: NonEmptyList[LocalTime]): EventPipe =
    new EventPipe {
      override def apply(event: Event): Option[Event] =
        event match {
          case MetricsSnapshot(_, _, index, _, _) =>
            val isKeep = index match {
              case Adhoc(_)       => true
              case Periodic(tick) =>
                localTimes.exists { lt =>
                  val zdt = LocalDateTime.of(tick.local(_.conclude).toLocalDate, lt).atZone(tick.zoneId)
                  tick.isWithinOpenClosed(zdt.toInstant)
                }
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

  /** Thin periodic `MetricsSnapshot`s to roughly one per `interval` elapsed since launch; `Adhoc` snapshots
    * and non-snapshot events always pass through.
    *
    * For each tick, the number of whole `interval`s since `launchTime` picks an expected boundary instant;
    * the tick is kept when it covers that boundary. Unlike [[indexFilter]] (which counts snapshots), this
    * filters by elapsed wall-clock time, so it is robust to changes in snapshot frequency.
    *
    * @param interval
    *   the target spacing between kept snapshots.
    */
  def windowFilter(interval: FiniteDuration): EventPipe =
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
