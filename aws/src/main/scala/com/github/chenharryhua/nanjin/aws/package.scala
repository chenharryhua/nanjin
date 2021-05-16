package com.github.chenharryhua.nanjin

import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.string.Url

package object aws {
  type SqsUrl = String Refined Url
  object SqsUrl extends RefinedTypeOps[SqsUrl, String] with CatsRefinedTypeOpsSyntax

  final case class SnsArn(value: String) extends AnyVal

}
