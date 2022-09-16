package com.github.chenharryhua.nanjin.common

import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.string.Url

object guard {
  type ServiceName = Refined[String, NameConstraint]
  object ServiceName extends RefinedTypeOps[ServiceName, String] with CatsRefinedTypeOpsSyntax

  type TaskName = Refined[String, NameConstraint]
  object TaskName extends RefinedTypeOps[TaskName, String] with CatsRefinedTypeOpsSyntax

  type Name = Refined[String, NameConstraint]
  object Name extends RefinedTypeOps[Name, String] with CatsRefinedTypeOpsSyntax

  type HomePage      = Refined[String, Url]
  type QueueCapacity = Refined[Int, NonNegative]
  type MaxRetry      = Refined[Int, NonNegative]
}
