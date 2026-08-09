package com.github.chenharryhua.nanjin.terminals

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}

object objectMapper extends ObjectMapper with ClassTagExtensions {
  this.registerModules(DefaultScalaModule)
}
