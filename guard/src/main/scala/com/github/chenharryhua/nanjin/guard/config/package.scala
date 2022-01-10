package com.github.chenharryhua.nanjin.guard

import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.boolean.And
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.char.LowerCase
import eu.timepit.refined.collection.{Forall, NonEmpty}
import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.string.{Trimmed, Url}

package object config {
  type ServiceName = Refined[String, NonEmpty And Trimmed]
  object ServiceName extends RefinedTypeOps[ServiceName, String] with CatsRefinedTypeOpsSyntax

  type AppName = Refined[String, NonEmpty And Trimmed]
  object AppName extends RefinedTypeOps[AppName, String] with CatsRefinedTypeOpsSyntax

  type Span = Refined[String, NonEmpty And Trimmed]
  object Span extends RefinedTypeOps[Span, String] with CatsRefinedTypeOpsSyntax

  type HomePage      = Refined[String, Url]
  type QueueCapacity = Refined[Int, NonNegative]
  type Catalog       = Refined[String, Forall[LowerCase]]
  type MaxRetry      = Refined[Int, NonNegative]
}
