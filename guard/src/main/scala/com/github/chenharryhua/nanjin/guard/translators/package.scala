package com.github.chenharryhua.nanjin.guard

import com.github.chenharryhua.nanjin.common.DurationFormatter
import io.circe.Json
import monocle.function.Plated
import io.circe.optics.JsonOptics.*
import java.text.DecimalFormat

package object translators {
  final private[translators] val fmt: DurationFormatter = DurationFormatter.defaultFormatter
  final private[translators] val decFmt: DecimalFormat  = new DecimalFormat("#,###.##")
  final private[translators] val prettyNumber: Json => Json = Plated.transform[Json] { js =>
    js.asNumber match {
      case Some(value) => Json.fromString(decFmt.format(value.toDouble))
      case None        => js
    }
  }
}
