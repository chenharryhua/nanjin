package com.github.chenharryhua.nanjin.guard.translators

import com.github.chenharryhua.nanjin.guard.event.*
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.Importance

sealed trait ColorScheme
object ColorScheme {
  case object GoodColor extends ColorScheme // successful-ish
  case object InfoColor extends ColorScheme // fyi
  case object WarnColor extends ColorScheme // well, not so wrong
  case object ErrorColor extends ColorScheme // oops
}

final class Coloring(f: ColorScheme => String) extends (NJEvent => String) {
  private def toScheme(evt: NJEvent): ColorScheme = evt match {
    case _: ServiceStart          => ColorScheme.InfoColor
    case _: ServicePanic          => ColorScheme.ErrorColor
    case ServiceStop(_, _, cause) => if (cause.exitCode === 0) ColorScheme.WarnColor else ColorScheme.ErrorColor
    case mr @ MetricReport(_, _, _, _, snapshot, _) =>
      if (!mr.isUp) ColorScheme.ErrorColor
      else if (snapshot.isContainErrors) ColorScheme.WarnColor
      else ColorScheme.InfoColor
    case _: MetricReset => ColorScheme.InfoColor
    case _: ActionStart => ColorScheme.InfoColor
    case _: ActionRetry => ColorScheme.WarnColor
    case _: ActionFail  => ColorScheme.ErrorColor
    case _: ActionSucc  => ColorScheme.GoodColor
    case InstantAlert(_, _, _, importance, _) =>
      importance match {
        case Importance.Critical => ColorScheme.ErrorColor
        case Importance.High     => ColorScheme.WarnColor
        case Importance.Medium   => ColorScheme.InfoColor
        case Importance.Low      => ColorScheme.InfoColor
      }
    case PassThrough(_, _, _, asError, _) => if (asError) ColorScheme.ErrorColor else ColorScheme.InfoColor
  }

  override def apply(evt: NJEvent): String = f(toScheme(evt))
}
