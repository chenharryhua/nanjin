package com.github.chenharryhua.nanjin.common

trait UpdateConfig[A, B] {
  def updateConfig(f: A => A): B
}
