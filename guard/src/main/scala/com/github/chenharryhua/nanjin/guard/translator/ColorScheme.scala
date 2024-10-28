package com.github.chenharryhua.nanjin.guard.translator

import cats.data.Cont
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.CounterKind
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, Category}
import com.github.chenharryhua.nanjin.guard.event.{
  retrieveHealthChecks,
  MetricSnapshot,
  NJEvent,
  ServiceStopCause
}
import enumeratum.values.{CatsOrderValueEnum, IntEnum, IntEnumEntry}

sealed abstract class ColorScheme(override val value: Int) extends IntEnumEntry

object ColorScheme extends CatsOrderValueEnum[Int, ColorScheme] with IntEnum[ColorScheme] {
  import NJEvent.*
  case object GoodColor extends ColorScheme(0) // successful-ish
  case object InfoColor extends ColorScheme(1) // fyi
  case object WarnColor extends ColorScheme(2) // well, not so wrong
  case object ErrorColor extends ColorScheme(3) // oops
  val values: IndexedSeq[ColorScheme] = findValues

  private def color_snapshot(ss: MetricSnapshot): ColorScheme = {
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

  def decorate[A](evt: NJEvent): Cont[A, ColorScheme] =
    Cont.pure[A, NJEvent](evt).map {
      case _: ServiceStart => InfoColor
      case _: ServicePanic => ErrorColor
      case ServiceStop(_, _, cause) =>
        cause match {
          case ServiceStopCause.Successfully   => GoodColor
          case ServiceStopCause.ByCancellation => WarnColor
          case ServiceStopCause.ByException(_) => ErrorColor
          case ServiceStopCause.Maintenance    => InfoColor
        }
      case ServiceMessage(_, _, _, level, _) =>
        level match {
          case AlarmLevel.Error => ErrorColor
          case AlarmLevel.Warn  => WarnColor
          case AlarmLevel.Info  => InfoColor
          case AlarmLevel.Done  => GoodColor
        }
      case MetricReport(_, _, ss, _) => color_snapshot(ss)
      case MetricReset(_, _, ss, _)  => color_snapshot(ss)
    }
}
