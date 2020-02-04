package com.github.chenharryhua.nanjin.spark

import monocle.macros.Lenses

final case class Repartition(value: Int) extends AnyVal

@Lenses final case class ShowSparkDataset(rowNum: Int, isTruncate: Boolean)
