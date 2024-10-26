package com.github.chenharryhua.nanjin.common

import cats.Endo

trait UpdateConfig[A, B] {
  def updateConfig(f: Endo[A]): B
}

trait EnableConfig[A] {
  def enable(isEnabled: Boolean): A
}
