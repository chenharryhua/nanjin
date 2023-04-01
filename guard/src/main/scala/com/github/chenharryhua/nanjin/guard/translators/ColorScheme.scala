package com.github.chenharryhua.nanjin.guard.translators

import cats.data.Cont
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.{AlertLevel, Category, CounterKind}
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, NJEvent}
import enumeratum.values.{CatsOrderValueEnum, IntEnum, IntEnumEntry}

sealed abstract class ColorScheme(override val value: Int) extends IntEnumEntry

object ColorScheme extends CatsOrderValueEnum[Int, ColorScheme] with IntEnum[ColorScheme] {
  import NJEvent.*
  case object GoodColor extends ColorScheme(0) // successful-ish
  case object InfoColor extends ColorScheme(1) // fyi
  case object WarnColor extends ColorScheme(2) // well, not so wrong
  case object ErrorColor extends ColorScheme(3) // oops
  val values: IndexedSeq[ColorScheme] = findValues

  private def colorSnapshot(ss: MetricSnapshot): ColorScheme =
    ss.counters
      .filter(_.count > 0)
      .mapFilter(_.metricId.category match {
        case Category.Counter(sub) =>
          sub.map {
            case CounterKind.ActionFail   => ErrorColor
            case CounterKind.ActionRetry  => WarnColor
            case CounterKind.AlertError   => ErrorColor
            case CounterKind.AlertWarn    => WarnColor
            case CounterKind.ErrorCounter => ErrorColor
            case _                        => InfoColor
          }
        case _ => Some(InfoColor)
      })
      .foldLeft(InfoColor: ColorScheme) { case (s, i) => s.max(i) }

  def decorate[A](evt: NJEvent): Cont[A, ColorScheme] =
    Cont.pure[A, NJEvent](evt).map {
      case _: ServiceStart          => InfoColor
      case _: ServicePanic          => ErrorColor
      case ServiceStop(_, _, cause) => if (cause.exitCode === 0) GoodColor else ErrorColor
      case _: ActionStart           => InfoColor
      case _: ActionRetry           => WarnColor
      case _: ActionFail            => ErrorColor
      case _: ActionComplete        => GoodColor
      case InstantAlert(_, _, _, alertLevel, _) =>
        alertLevel match {
          case AlertLevel.Error => ErrorColor
          case AlertLevel.Warn  => WarnColor
          case AlertLevel.Info  => InfoColor
        }
      case MetricReport(_, _, _, ss) => colorSnapshot(ss)
      case MetricReset(_, _, _, ss)  => colorSnapshot(ss)
    }
}
