package com.github.chenharryhua.nanjin.spark

import monocle.macros.Lenses

final private[spark] case class NJRepartition(value: Int) extends AnyVal
final private[spark] case class NJPath(value: String) extends AnyVal
final private[spark] case class NJCheckpoint(value: String) extends AnyVal
final private[spark] case class NJFailOnDataLoss(value: Boolean) extends AnyVal

@Lenses final private[spark] case class NJShowDataset(rowNum: Int, isTruncate: Boolean)
