package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import monocle.macros.Lenses

@Lenses final private[spark] case class NJShowDataset(rowNum: Int, isTruncate: Boolean)
