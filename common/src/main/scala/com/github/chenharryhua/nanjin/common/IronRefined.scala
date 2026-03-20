package com.github.chenharryhua.nanjin.common

import io.github.iltotore.iron.RefinedType

object IronRefined {
  trait PlusConversion[A, C] {
    rt: RefinedType[A, C] =>
    given Conversion[A, rt.T] with
      override def apply(a: A): rt.T = rt.applyUnsafe(a)
  }

}
