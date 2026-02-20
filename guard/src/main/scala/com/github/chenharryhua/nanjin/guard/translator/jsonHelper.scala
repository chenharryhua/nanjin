package com.github.chenharryhua.nanjin.guard.translator

import com.github.chenharryhua.nanjin.guard.config.Attribute
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceMessage
import io.circe.Json
import io.circe.syntax.EncoderOps

object jsonHelper {

  def json_service_message(sm: ServiceMessage): Json = {
    val serviceInfo: List[(String, Json)] =
      List(
        Attribute(sm.serviceParams.serviceName).snakeJsonEntry,
        Attribute(sm.serviceParams.serviceId).snakeJsonEntry,
        Attribute(sm.domain).snakeJsonEntry,
        Attribute(sm.correlation).snakeJsonEntry
      )

    sm.stackTrace match {
      case Some(err) =>
        List(sm.message, (Attribute(err).snakeJsonEntry :: serviceInfo).toMap.asJson).asJson
      case None =>
        List(sm.message, serviceInfo.toMap.asJson).asJson
    }
  }
}
