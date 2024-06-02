package com.github.chenharryhua.nanjin.guard.translator

import cats.Eval
import com.github.chenharryhua.nanjin.guard.event.NJEvent

object htmlHelper {
  def htmlColoring(evt: NJEvent): String = ColorScheme
    .decorate(evt)
    .run {
      case ColorScheme.GoodColor  => Eval.now("color:darkgreen")
      case ColorScheme.InfoColor  => Eval.now("color:black")
      case ColorScheme.WarnColor  => Eval.now("color:#b3b300")
      case ColorScheme.ErrorColor => Eval.now("color:red")
    }
    .value

}
