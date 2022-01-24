package com.github.chenharryhua.nanjin.guard

import cats.data.NonEmptyList
import com.amazonaws.thirdparty.apache.codec.digest.DigestUtils
import com.github.chenharryhua.nanjin.common.NameConstraint
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.char.LowerCase
import eu.timepit.refined.collection.Forall
import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.string.Url

package object config {

  type ServiceName = Refined[String, NameConstraint]
  object ServiceName extends RefinedTypeOps[ServiceName, String] with CatsRefinedTypeOpsSyntax

  type AppName = Refined[String, NameConstraint]
  object AppName extends RefinedTypeOps[AppName, String] with CatsRefinedTypeOpsSyntax

  type Span = Refined[String, NameConstraint]
  object Span extends RefinedTypeOps[Span, String] with CatsRefinedTypeOpsSyntax

  type HomePage      = Refined[String, Url]
  type QueueCapacity = Refined[Int, NonNegative]
  type Catalog       = Refined[String, Forall[LowerCase]]
  type MaxRetry      = Refined[Int, NonNegative]

  def digestSpans(spans: NonEmptyList[Span]): String =
    DigestUtils.sha1Hex(spans.toList.map(_.value).mkString("/")).take(8)
}
