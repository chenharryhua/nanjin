package com.github.chenharryhua.nanjin.guard.translators

import com.github.chenharryhua.nanjin.guard.event.ServiceStart
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.circe.syntax.EncoderOps

private[translators] object SimpleJsonTranslator {
  private def serviceStarted(evt: ServiceStart): Json =
    json"""{
       "timestamp": ${evt.timestamp.asJson}   
    }"""
}
