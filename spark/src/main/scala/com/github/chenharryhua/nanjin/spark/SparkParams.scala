package com.github.chenharryhua.nanjin.spark

import monocle.macros.Lenses

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

final case class StorageRootPath(value: String) extends AnyVal

@Lenses final case class UploadRate(batchSize: Int, duration: FiniteDuration)

object UploadRate {
  val default: UploadRate = UploadRate(1000, 1.second)
}

private[spark] trait UpdateParams[A, B] {
  def updateParams(f: A => A): B
}
