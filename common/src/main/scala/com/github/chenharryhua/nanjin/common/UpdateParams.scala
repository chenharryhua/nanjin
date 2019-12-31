package com.github.chenharryhua.nanjin.common

trait UpdateParams[A, B] {
  def updateParams(f: A => A): B
}
