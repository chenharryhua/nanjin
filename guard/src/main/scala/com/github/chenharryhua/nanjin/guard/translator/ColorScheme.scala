package com.github.chenharryhua.nanjin.guard.translator

import cats.Defer
import cats.data.ContT
import cats.syntax.order.catsSyntaxOrder
import com.github.chenharryhua.nanjin.guard.config.AlarmLevel
import com.github.chenharryhua.nanjin.guard.event.CategoryKind.CounterKind
import com.github.chenharryhua.nanjin.guard.event.{
  retrieveHealthChecks,
  Category,
  Event,
  Snapshot,
  StopReason
}
import enumeratum.values.{CatsOrderValueEnum, IntEnum, IntEnumEntry}

sealed abstract class ColorScheme(override val value: Int) extends IntEnumEntry

object ColorScheme extends CatsOrderValueEnum[Int, ColorScheme] with IntEnum[ColorScheme] {
  import Event.*
  case object DebugColor extends ColorScheme(0) // refer to AlarmLevel.Debug
  case object InfoColor extends ColorScheme(1) // fyi
  case object GoodColor extends ColorScheme(2) // successful-ish
  case object WarnColor extends ColorScheme(3) // well, not so wrong
  case object ErrorColor extends ColorScheme(4) // oops
  val values: IndexedSeq[ColorScheme] = findValues

  private def color_snapshot(ss: Snapshot): ColorScheme = {
    val gauge_color: ColorScheme =
      if (retrieveHealthChecks(ss.gauges).values.forall(identity)) {
        InfoColor
      } else {
        ErrorColor
      }

    val counter_color: ColorScheme =
      ss.counters
        .filter(_.count > 0)
        .collect(_.metricId.category match { case Category.Counter(kind) => kind })
        .map {
          case CounterKind.Risk => WarnColor
          case _                => InfoColor
        }
        .foldLeft(InfoColor: ColorScheme) { case (s, i) => s.max(i) }

    counter_color.max(gauge_color)
  }

  def decorate[F[_]: Defer, A](evt: Event): ContT[F, A, ColorScheme] =
    ContT.pure[F, A, Event](evt).map {
      case _: ServiceStart => InfoColor
      case _: ServicePanic => ErrorColor
      case ss: ServiceStop =>
        ss.cause match {
          case StopReason.Successfully   => GoodColor
          case StopReason.ByCancellation => WarnColor
          case StopReason.ByException(_) => ErrorColor
          case StopReason.Maintenance    => InfoColor
        }
      case sm: ReportedEvent =>
        sm.level match {
          case AlarmLevel.Error => ErrorColor
          case AlarmLevel.Warn  => WarnColor
          case AlarmLevel.Info  => InfoColor
          case AlarmLevel.Good  => GoodColor
          case AlarmLevel.Debug => DebugColor
        }
      case mr: MetricsEvent => color_snapshot(mr.snapshot)
    }
}
