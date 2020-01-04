package com.github.chenharryhua.nanjin.common

import cats.Show
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.string.Uri
import monocle.macros.Lenses

import scala.concurrent.duration.{FiniteDuration, _}

final case class NJRootPath(uri: String Refined Uri) {
  val root: String = if (uri.value.endsWith("/")) uri.value else uri.value + "/"

  def +(sub: String): String = if (sub.startsWith("/")) root + sub.tail else root + sub

}

object NJRootPath {
  implicit val showNJRootPath: Show[NJRootPath] = x => s"NJRootPath(${x.root})"

  val default: NJRootPath = NJRootPath("./data/")
}

@Lenses final case class NJRate(batchSize: Int, duration: FiniteDuration)

object NJRate {
  val default: NJRate = NJRate(1000, 1.second)
}
