package com.github.chenharryhua.nanjin.guard

import com.github.chenharryhua.nanjin.common.DurationFormatter

import java.text.DecimalFormat

package object translator {

  final val durationFormatter: DurationFormatter = DurationFormatter.defaultFormatter
  final val decimalFormatter: DecimalFormat = new DecimalFormat("#,###")

}
