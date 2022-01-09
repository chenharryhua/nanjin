package com.github.chenharryhua.nanjin.guard

import eu.timepit.refined.api.Refined
import eu.timepit.refined.boolean.{And, Not}
import eu.timepit.refined.char.LowerCase
import eu.timepit.refined.collection.{Contains, Forall, NonEmpty}
import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.string.{Trimmed, Url}

package object config {
  type ServiceName   = Refined[String, NonEmpty And Trimmed]
  type AppName       = Refined[String, NonEmpty And Trimmed]
  type HomePage      = Refined[String, Url]
  type QueueCapacity = Refined[Int, NonNegative]
  type Catalog       = Refined[String, Forall[LowerCase]]
  type MaxRetry      = Refined[Int, NonNegative]
  type Span          = Refined[String, NonEmpty And Trimmed And Not[Contains['/']]]
}
