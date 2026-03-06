package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.syntax.show.toShow
import com.comcast.ip4s.Port
import com.github.chenharryhua.nanjin.common.chrono.Policy
import io.circe.Json
import io.circe.syntax.EncoderOps
import scalatags.Text
import scalatags.Text.all.{raw, script}

import java.time.ZoneId

/*
 *  Corresponding Config in Frontend defined at:
 * `com.github.chenharryhua.nanjin.frontend.BackendConfig`
 */

final case class BackendConfig(
  serviceName: String,
  port: Port,
  zoneId: ZoneId,
  maxPoints: Int,
  policy: Policy) {
  private val no_spaces_json = Json.obj(
    "serviceName" -> Json.fromString(serviceName),
    "port" -> Json.fromInt(port.value),
    "zoneId" -> zoneId.asJson,
    "maxPoints" -> Json.fromInt(maxPoints),
    "policy" -> policy.show.asJson
  ).noSpaces

  val config: Text.TypedTag[String] =
    script(
      raw(
        s"""
           |window.BACKEND_CONFIG = Object.freeze($no_spaces_json);
       """.stripMargin
      ))

}
