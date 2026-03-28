package com.github.chenharryhua.nanjin.guard.event

import cats.data.NonEmptyList
import cats.syntax.order.given
import com.github.chenharryhua.nanjin.guard.config.AlarmLevel
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index.{Adhoc, Periodic}
import cron4s.lib.javatime.javaTemporalInstance
import cron4s.{toDateTimeCronOps, CronExpr}

import java.time.{Duration, Instant, LocalDateTime, LocalTime}
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

// mapFilter friendly
trait EventPipe { self =>
  def apply(event: Event): Option[Event]

  final def filter(event: Event): Boolean =
    apply(event).isDefined

  final def &&(other: EventPipe): EventPipe =
    new EventPipe {
      def apply(event: Event): Option[Event] =
        self(event).flatMap(other(_))
    }
}

object EventPipe {

  def apply(f: Event => Option[Event]): EventPipe =
    new EventPipe { override def apply(e: Event): Option[Event] = f(e) }

  val identity: EventPipe = new EventPipe {
    override def apply(event: Event): Option[Event] = Some(event)
  }

  def alarmLevel(f: AlarmLevel.type => AlarmLevel): EventPipe =
    new EventPipe {
      private val threshold: AlarmLevel = f(AlarmLevel)
      override def apply(event: Event): Option[Event] =
        event match {
          case evt @ Event.ReportedEvent(_, _, _, _, level, _, _) =>
            if level >= threshold then Some(evt) else None
          case other => Some(other)
        }
    }

  def noAdhoc: EventPipe =
    new EventPipe {
      override def apply(event: Event): Option[Event] = event match {
        case Event.MetricsSnapshot(index, _, _, _, _) =>
          index match {
            case Adhoc(_)    => None
            case Periodic(_) => Some(event)
          }
        case others => Some(others)
      }
    }

  def cronFilter(cronExpr: CronExpr): EventPipe =
    new EventPipe {
      override def apply(event: Event): Option[Event] =
        event match {
          case MetricsSnapshot(index, _, _, _, _) =>
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

  def localTimeFilter(localTimes: NonEmptyList[LocalTime]): EventPipe =
    new EventPipe {
      override def apply(event: Event): Option[Event] =
        event match {
          case MetricsSnapshot(index, _, _, _, _) =>
            val isKeep = index match {
              case Adhoc(_)       => true
              case Periodic(tick) =>
                localTimes.exists { lt =>
                  val zdt = LocalDateTime.of(tick.local(_.conclude).toLocalDate, lt).atZone(tick.zoneId)
                  if tick.isWithinOpenClosed(zdt.toInstant) then true else false
                }
            }
            if isKeep then Some(event) else None
          case others => Some(others)
        }
    }

  def indexFilter(divisor: Int): EventPipe =
    new EventPipe {
      override def apply(event: Event): Option[Event] =
        event match {
          case MetricsSnapshot(index, _, _, _, _) =>
            index match {
              case Adhoc(_)       => Some(event)
              case Periodic(tick) => if ((tick.index % divisor) === 0) Some(event) else None
            }
          case other => Some(other)
        }
    }

  def windowFilter(interval: FiniteDuration): EventPipe =
    new EventPipe {

      override def apply(event: Event): Option[Event] =
        event match {
          case MetricsSnapshot(index, _, _, _, _) =>
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
