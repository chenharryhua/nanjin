package com.github.chenharryhua.nanjin.guard

import com.github.chenharryhua.nanjin.common.DurationFormatter

import java.text.DecimalFormat

package object translator {

  final private[guard] val fmt: DurationFormatter     = DurationFormatter.defaultFormatter
  final private[guard] val decimal_fmt: DecimalFormat = new DecimalFormat("#,###.##")

}
