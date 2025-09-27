package com.github.chenharryhua.nanjin.messages.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
/*
 *Use one configured ObjectMapper per application (or per distinct configuration).
 * It’s designed for concurrent use.
 */
object globalObjectMapper extends ObjectMapper with ClassTagExtensions {
  this.registerModules(DefaultScalaModule)
}
