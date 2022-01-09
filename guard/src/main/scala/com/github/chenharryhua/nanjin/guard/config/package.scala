package com.github.chenharryhua.nanjin.guard

import eu.timepit.refined.api.Refined
import eu.timepit.refined.boolean.And
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.string.{Trimmed, Url}

package object config {
  type ServiceName   = Refined[String, NonEmpty And Trimmed]
  type AppName       = Refined[String, NonEmpty And Trimmed]
  type HomePage      = Refined[String, Url]
  type QueueCapacity = Refined[Int, NonNegative]
}
