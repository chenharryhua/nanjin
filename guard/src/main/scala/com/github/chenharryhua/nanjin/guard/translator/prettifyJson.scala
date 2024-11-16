package com.github.chenharryhua.nanjin.guard.translator

import io.circe.optics.all.*
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import monocle.function.Plated
import org.apache.commons.lang3.StringUtils

import java.time.Duration

object prettifyJson {

  private val pretty_json: Json => Json =
    Plated.transform[Json] { js =>
      js.asNumber match {
        case Some(value) => Json.fromString(decimal_fmt.format(value.toDouble))
        case None =>
          js.as[Duration] match {
            case Left(_)      => js
            case Right(value) => Json.fromString(fmt.format(value))
          }
      }
    }

  def apply[A: Encoder](a: A): Json = pretty_json(a.asJson)

  def ymlShow(json: Json): List[String] =
    json.fold(
      jsonNull = Nil,
      jsonBoolean = jb => List(jb.toString),
      jsonNumber = jn => List(jn.toString),
      jsonString = js => List(js),
      jsonArray = vj => List(s"[${vj.flatMap(ymlShow).mkString(", ")}]"),
      jsonObject = { jo =>
        val jsMap: Map[String, Json] = jo.toMap
        val maxLength: Int           = jsMap.keys.map(_.length).max
        jsMap.map { case (key, value) =>
          s"${StringUtils.rightPad(key, maxLength)}: ${ymlShow(value).mkString(", ")}"
        }.toList
      }
    )

}
