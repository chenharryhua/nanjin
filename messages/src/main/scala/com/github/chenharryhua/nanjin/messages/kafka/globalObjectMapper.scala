package com.github.chenharryhua.nanjin.messages.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
/*
 *Use one configured ObjectMapper per application (or per distinct configuration).
 * It’s designed for concurrent use.
 */
object globalObjectMapper extends ObjectMapper with ClassTagExtensions {
  this
    .registerModules(new ParameterNamesModule)
    .registerModule(new Jdk8Module)
    .registerModule(new JavaTimeModule)
    .registerModules(DefaultScalaModule)
}
