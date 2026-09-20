package com.github.chenharryhua.nanjin.common

import cats.Endo

/** Mixin providing an `updateConfig` method to modify an internal configuration of type `A`. */
trait UpdateConfig[A, B] {
  def updateConfig(f: Endo[A]): B
}

/** Mixin providing a boolean enable/disable toggle. */
trait EnableConfig[A] {
  def enable(isEnabled: Boolean): A
}

/** Mixin exposing a string-keyed property map, typically for Java client configuration. */
trait HasProperties {
  def properties: Map[String, String]
}
