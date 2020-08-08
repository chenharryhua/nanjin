package com.github.chenharryhua.nanjin.spark

import monocle.macros.Lenses

final private[spark] case class NJRepartition(value: Int) extends AnyVal

final private[spark] case class NJPath(value: String) extends AnyVal {

  def append(sub: String): NJPath = {
    val s = if (sub.startsWith("/")) sub.tail else sub
    val v = if (value.endsWith("/")) value.dropRight(1) else value
    NJPath(s"$v/$s")
  }
}

@Lenses final private[spark] case class NJShowDataset(rowNum: Int, isTruncate: Boolean)

final private[spark] case class NJCheckpoint(value: String) extends AnyVal {

  def append(sub: String): NJCheckpoint = {
    val s = if (sub.startsWith("/")) sub.tail else sub
    val v = if (value.endsWith("/")) value.dropRight(1) else value
    NJCheckpoint(s"$v/$s")
  }
}

final private[spark] case class NJFailOnDataLoss(value: Boolean) extends AnyVal
