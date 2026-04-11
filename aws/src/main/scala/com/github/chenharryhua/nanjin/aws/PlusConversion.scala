package com.github.chenharryhua.nanjin.aws

import io.github.iltotore.iron.RefinedType

trait PlusConversion[A, C] {
  rt: RefinedType[A, C] =>
  given Conversion[A, rt.T] with
    override def apply(a: A): rt.T = rt.applyUnsafe(a)
}
