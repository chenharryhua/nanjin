package com.github.chenharryhua.nanjin.guard

import com.github.chenharryhua.nanjin.common.DurationFormatter
import org.apache.commons.lang3.StringUtils

import java.text.DecimalFormat

package object translator {
  private[translator] val space: String = StringUtils.SPACE

  final val durationFormatter: DurationFormatter = DurationFormatter.defaultFormatter
  final val decimalFormatter: DecimalFormat = new DecimalFormat("#,###")

}
