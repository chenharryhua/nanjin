package com.github.chenharryhua.nanjin.guard.translators

import cats.data.Cont
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.Importance
import com.github.chenharryhua.nanjin.guard.event.NJEvent

sealed trait ColorScheme extends Product with Serializable
object ColorScheme {
  import NJEvent.*
  case object GoodColor extends ColorScheme // successful-ish
  case object InfoColor extends ColorScheme // fyi
  case object WarnColor extends ColorScheme // well, not so wrong
  case object ErrorColor extends ColorScheme // oops

  def decorate[A](evt: NJEvent): Cont[A, ColorScheme] =
    Cont.pure[A, NJEvent](evt).map {
      case _: ServiceStart          => InfoColor
      case _: ServicePanic          => ErrorColor
      case ServiceStop(_, _, cause) => if (cause.exitCode === 0) GoodColor else ErrorColor
      case MetricReport(_, _, _, snapshot) =>
        if (snapshot.counterMap.filter(_._2 > 0).keys.exists(_.startsWith("0"))) WarnColor else InfoColor
      case _: MetricReset => InfoColor
      case _: ActionStart => InfoColor
      case _: ActionRetry => WarnColor
      case _: ActionFail  => ErrorColor
      case _: ActionSucc  => GoodColor
      case InstantAlert(_, _, _, importance, _) =>
        importance match {
          case Importance.Critical => ErrorColor
          case Importance.High     => WarnColor
          case Importance.Medium   => InfoColor
          case Importance.Low      => InfoColor
        }
      case PassThrough(_, _, _, isError, _) => if (isError) ErrorColor else InfoColor
    }
}
