package com.github.chenharryhua.nanjin.spark

import monocle.macros.Lenses

final case class NJRepartition(value: Int) extends AnyVal
final case class NJPath(value: String) extends AnyVal
final case class NJCheckpoint(value: String) extends AnyVal

@Lenses final case class NJShowDataset(rowNum: Int, isTruncate: Boolean)
